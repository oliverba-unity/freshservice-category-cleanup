import datetime
import json
import time
import threading
import concurrent.futures
import sqlite3

from freshservice_api.freshservice_api import FreshserviceApi

class TicketImporter(object):

    def __init__(self, fs_api: FreshserviceApi, db_filename: str="import.sqlite"):
        self.fs_api = fs_api
        self.db_filename = db_filename
        self.iteration_count = 0
        self.iteration_limit = None
        self.success_count = 0
        self.failure_count = 0
        self.count_lock = threading.Lock()  # Prevent threads from simultaneously incrementing the counter
        self.print_lock = threading.Lock() # Prevent threads from simultaneously outputting to the console
        self.start_time = None

    def create_tables(self):
        create_tickets_table = """
        CREATE TABLE IF NOT EXISTS tickets (
            id INTEGER PRIMARY KEY,
            email TEXT,
            subject TEXT,
            description TEXT,
            category TEXT,
            sub_category TEXT,
            item_category TEXT,
            request_timestamp TIMESTAMP DEFAULT NULL,
            response_ticket_id INTEGER,
            response_status_code INTEGER DEFAULT NULL,
            error_message TEXT DEFAULT NULL
        );
        """
        db = sqlite3.connect(database=self.db_filename, timeout=30.0)
        db.row_factory = sqlite3.Row

        db.execute(create_tickets_table)

        print("Created tickets table.")

    def run(self, limit: int=None, max_workers: int=10):

        print(f"Starting ticket importer with {max_workers} worker threads...")

        self.start_time = time.time()
        self.iteration_count = 0

        if limit:
            self.iteration_limit = int(limit)
            print(f"Limit set to {limit} tickets.")

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for i in range(max_workers):
                futures.append(
                    executor.submit(self._ticket_import_worker)
                )

            # As futures finish, return their result, to re-raise and catch any exceptions
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Worker thread failed: {e}")

        finish_time = time.time()
        final_duration = finish_time - self.start_time
        self.fs_api.close()

        total_count = self.success_count + self.failure_count

        print(f"Processed {total_count} tickets in {final_duration:.2f} seconds.")
        print(f"{self.failure_count} failed to import.")
        print(f"{self.success_count} successfully imported.")

        if final_duration > 0:
            print(f"Overall requests per minute: {(total_count / final_duration) * 60:.2f}")

    def retry_failed(self):
        db = sqlite3.connect(database=self.db_filename, timeout=30.0)
        db.row_factory = sqlite3.Row

        query = """
        UPDATE tickets
        SET request_timestamp = NULL,
            response_status_code = NULL,
            error_message = NULL
        WHERE response_status_code IS NOT 201;
        """

        result = db.execute(query)
        db.commit()

        quantity_to_retry = result.rowcount

        if quantity_to_retry > 0:
            print(f"Retrying import of {quantity_to_retry} tickets...")
            self.run()
        else:
            print(f"No tickets to retry.")

    def _ticket_import_worker(self):
        db = sqlite3.connect(database=self.db_filename, timeout=30.0)
        db.row_factory = sqlite3.Row

        while True:

            with self.count_lock:
                if self.iteration_limit is not None and self.iteration_count >= self.iteration_limit:
                    break
                self.iteration_count += 1

            ticket_row = None

            try:
                db.execute("BEGIN IMMEDIATE")
                next_ticket_query = """
                        SELECT *
                        FROM tickets
                        WHERE request_timestamp IS NULL
                        ORDER BY id DESC
                        LIMIT 1;
                        """
                cursor = db.execute(next_ticket_query)

                ticket_row = cursor.fetchone()

                if not ticket_row:
                    db.rollback()
                    break

                in_progress_query = """
                   UPDATE tickets
                   SET request_timestamp = ?
                   WHERE id = ?; \
                   """

                db.execute(in_progress_query, (datetime.datetime.now(), ticket_row['id']))
                db.commit()
            except sqlite3.OperationalError:
                db.rollback()

            ticket_payload = {
                "email": ticket_row["email"],
                "subject": ticket_row["subject"],
                "description": ticket_row["description"],
                "source": 1002, # API
                "category": ticket_row['category']
            }

            if ticket_row['sub_category']:
                ticket_payload["sub_category"] = ticket_row['sub_category']
            if ticket_row['item_category']:
                ticket_payload["item_category"] = ticket_row['item_category']

            try:
                response = self.fs_api.ticket().create(ticket_payload)

                status_code = response.status_code
                response_json = response.json()
                response_ticket_id = response_json.get('ticket', {}).get('id')

                query = """
                        UPDATE tickets
                        SET response_ticket_id   = ?,
                            response_status_code = ?
                        WHERE id = ?;
                        """

                db.execute(query, (response_ticket_id, status_code, ticket_row['id']))
                db.commit()

                self.success_count = self.success_count + 1

            except Exception as e:
                error_message = str(e)
                status_code = None

                self.failure_count = self.failure_count + 1

                if hasattr(e, 'response') and e.response is not None:
                    status_code = e.response.status_code
                    try:
                        # Try to get a clean error message from JSON
                        error_json = e.response.json()
                        error_message = json.dumps(error_json)
                    except:
                        error_message = e.response.text

                query_update = """
                               UPDATE tickets
                               SET response_status_code = ?,
                                   error_message        = ?
                               WHERE id = ?;
                               """
                db.execute(query_update, (status_code, error_message, ticket_row['id']))
                db.commit()

            self._print_progress(
                row_id=ticket_row['id'],
                status_code=status_code
            )

    def _print_progress(self, row_id: int, status_code: int | None = None):
        now = time.time()
        elapsed = now - self.start_time
        total_count = self.success_count + self.failure_count
        requests_per_minute = (total_count / elapsed) * 60 if elapsed > 0 else 0

        ratelimit_remaining = self.fs_api.controller.server_ratelimit_remaining
        ratelimit_total = self.fs_api.controller.server_ratelimit_total

        used = ratelimit_total - ratelimit_remaining
        pct = (used / ratelimit_total) * 100 if ratelimit_total > 0 else 0
        bar_len = 15
        filled = int(bar_len * pct / 100)
        bar = '█' * (bar_len - filled) + '-' * filled

        icon = ""

        match status_code:
            case _ if 200 <= status_code < 300:
                icon = "✅"
            case 429: # Rate Limited
                icon = "🚦"
            case _:
                icon = "❌"

        with self.print_lock:
            print(f"Ticket {row_id:06d} {icon} HTTP {status_code} "
                  f"[{bar}] Rate limit: {ratelimit_total} req/min. "
                  f"Remaining: {ratelimit_remaining:03d}. "
                  f"Current: {requests_per_minute:.1f} req/min")
