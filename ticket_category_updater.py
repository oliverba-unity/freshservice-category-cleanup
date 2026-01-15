import datetime
import json
import time
import threading
import concurrent.futures
import sqlite3
from typing import Optional

from freshservice_api.freshservice_api import FreshserviceApi

class TicketCategoryUpdater(object):

    def __init__(self, fs_api: FreshserviceApi, db_filename: str="ticket_category_update.sqlite"):
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
            category TEXT,
            sub_category TEXT,
            item_category TEXT,
            new_category TEXT,
            new_sub_category TEXT,
            new_item_category TEXT,
            update_state TEXT,
            request_timestamp TIMESTAMP DEFAULT NULL,
            response_status_code INTEGER DEFAULT NULL,
            error_message TEXT DEFAULT NULL
        );
        """

        create_valid_categories_table = """
        CREATE TABLE IF NOT EXISTS valid_categories (
            id INTEGER PRIMARY KEY,
            category TEXT NOT NULL,
            sub_category TEXT,
            item_category TEXT
        );
        """

        create_category_mappings_table = """
        CREATE TABLE IF NOT EXISTS category_mappings (
            id INTEGER PRIMARY KEY,
            old_category TEXT NOT NULL,
            old_sub_category TEXT,
            old_item_category TEXT,
            new_category TEXT,
            new_sub_category TEXT,
            new_item_category TEXT
        );
        """
        db = sqlite3.connect(database=self.db_filename, timeout=30.0)
        db.row_factory = sqlite3.Row
        db.execute(create_tickets_table)
        db.execute(create_valid_categories_table)
        db.execute(create_category_mappings_table)

        print("Created tables.")

    def prepare(self):
        db = sqlite3.connect(database=self.db_filename, timeout=30.0)
        db.row_factory = sqlite3.Row

        query = """
        SELECT * FROM tickets 
        WHERE update_state IS NULL
        ORDER BY id DESC 
        """

        tickets = db.execute(query).fetchall()

        skipped = 0
        unmapped = 0
        ready = 0
        total = 0

        for ticket in tickets:

            category_valid = self.validate_category(ticket["category"], ticket["sub_category"], ticket["item_category"])
            category_empty = all(v is None for v in [ticket["category"], ticket["sub_category"], ticket["item_category"]])

            if category_valid or category_empty:
                query = """
                        UPDATE tickets
                        SET update_state = 'skipped'
                        WHERE id = ?
                        """
                db.execute(query, (ticket["id"],))
                skipped = skipped + 1
            else:
                new = self.get_new_category(ticket["category"], ticket["sub_category"], ticket["item_category"])
                if new:
                    query = """
                            UPDATE tickets
                            SET update_state = 'ready',
                                new_category = ?,
                                new_sub_category = ?,
                                new_item_category = ?
                            WHERE id = ?;
                            """
                    db.execute(
                        query,
                        (
                            new["new_category"],
                            new["new_sub_category"],
                            new["new_item_category"],
                            ticket["id"]
                        )
                    )
                    ready = ready + 1
                else:
                    query = """
                            UPDATE tickets
                            SET update_state = 'unmapped'
                            WHERE id = ?;
                            """
                    db.execute(query, (ticket["id"],))
                    unmapped = unmapped + 1

            total = total + 1

        db.commit()

        print(f"Prepared {total} tickets:")
        print(f"{skipped} skipped - already have valid categories.")
        print(f"{unmapped} unmapped - no new category found for ticket's existing category.")
        print(f"{ready} ready to update via API.")

    def validate_category(
            self,
            category: str,
            sub_category: Optional[str]=None,
            item_category: Optional[str]=None
    ) -> bool:
        db = sqlite3.connect(database=self.db_filename, timeout=30.0)
        db.row_factory = sqlite3.Row

        if category and sub_category and item_category:
            query = """
            SELECT * FROM valid_categories 
            WHERE category = ?
            AND sub_category = ?
            AND item_category = ?
            LIMIT 1;
            """
            cursor = db.execute(query, (category, sub_category, item_category))
        elif category and sub_category:
            query = """
            SELECT * FROM valid_categories 
            WHERE category = ?
            AND sub_category = ?
            AND item_category IS NULL
            LIMIT 1;
            """
            cursor = db.execute(query, (category, sub_category))
        elif category:
            query = """
            SELECT * FROM valid_categories 
            WHERE category = ?
            AND sub_category IS NULL
            AND item_category IS NULL
            LIMIT 1;
            """
            cursor = db.execute(query, (category,))
        else:
            return False

        result = cursor.fetchone()

        if result is not None:
            return True
        return False

    def get_new_category(
            self,
             old_category: str,
             old_sub_category: Optional[str] = None,
             old_item_category: Optional[str] = None
    ) -> Optional[sqlite3.Row]:
        db = sqlite3.connect(database=self.db_filename, timeout=30.0)
        db.row_factory = sqlite3.Row

        if old_sub_category and old_item_category:
            query = """
                    SELECT *
                    FROM category_mappings
                    WHERE old_category = ?
                      AND old_sub_category = ?
                      AND old_item_category = ?
                    LIMIT 1;
                    """
            cursor = db.execute(query, (old_category, old_sub_category, old_item_category))
        elif old_sub_category:
            query = """
                    SELECT *
                    FROM category_mappings
                    WHERE old_category = ?
                      AND old_sub_category = ?
                      AND old_item_category IS NULL
                    LIMIT 1;
                    """
            cursor = db.execute(query, (old_category, old_sub_category))
        else:
            query = """
                    SELECT *
                    FROM category_mappings
                    WHERE old_category = ?
                      AND old_sub_category IS NULL
                      AND old_item_category IS NULL
                    LIMIT 1;
                    """
            cursor = db.execute(query, (old_category,))

        return cursor.fetchone()

    def run(self, limit: int=None, max_workers: int=10):
        print(f"Starting ticket category updater with {max_workers} worker threads...")

        db = sqlite3.connect(database=self.db_filename, timeout=30.0)
        db.row_factory = sqlite3.Row

        self.start_time = time.time()
        self.iteration_count = 0

        if limit:
            self.iteration_limit = int(limit)
            print(f"Limit set to {limit} tickets.")

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for i in range(max_workers):
                futures.append(
                    executor.submit(self._ticket_update_worker)
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

        print(f"Processed {self.iteration_count} tickets in {final_duration:.2f} seconds.")
        print(f"{self.failure_count} failed to update.")
        print(f"{self.success_count} successfully updated.")

        if final_duration > 0:
            print(f"Overall requests per minute: {(self.iteration_count / final_duration) * 60:.2f}")

    def retry_failed(self):
        db = sqlite3.connect(database=self.db_filename, timeout=30.0)
        db.row_factory = sqlite3.Row

        query = """
        UPDATE tickets
        SET update_state = NULL,
            request_timestamp = NULL,
            response_status_code = NULL,
            error_message = NULL
        WHERE update_state = 'failed';
        """

        result = db.execute(query)
        db.commit()

        quantity_to_retry = result.rowcount

        if quantity_to_retry > 0:
            print(f"Retrying updates for {quantity_to_retry} tickets...")
            self.run()
        else:
            print(f"No tickets to retry.")

    def _ticket_update_worker(self):
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
                    WHERE update_state = 'ready'
                    ORDER BY id DESC
                    LIMIT 1;
                    """
                ticket_row = db.execute(next_ticket_query).fetchone()

                if not ticket_row:
                    db.rollback()
                    break

                in_progress_query = """
                        UPDATE tickets
                        SET update_state      = 'in-progress',
                            request_timestamp = ?
                        WHERE id = ?;
                        """

                db.execute(in_progress_query, (datetime.datetime.now(), ticket_row['id']))
                db.commit()
            except sqlite3.OperationalError:
                db.rollback()

            ticket_update_payload = {"category": ticket_row['new_category']}
            if ticket_row['new_sub_category']:
                ticket_update_payload["sub_category"] = ticket_row['new_sub_category']
            if ticket_row['new_item_category']:
                ticket_update_payload["item_category"] = ticket_row['new_item_category']

            try:
                response = self.fs_api.ticket().update(ticket_row['id'], ticket_update_payload)

                status_code = response.status_code

                query = """
                        UPDATE tickets
                        SET update_state         = 'updated',
                            response_status_code = ?
                        WHERE id = ?;
                        """

                db.execute(query, (response.status_code, ticket_row['id']))
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

                update = """
                        UPDATE tickets
                        SET update_state            = 'failed',
                            response_status_code    = ?,
                            error_message           = ?
                        WHERE id = ?;
                        """

                db.execute(update,(status_code, error_message, ticket_row['id']))
                db.commit()

            self._print_progress(
                row_id=ticket_row['id'],
                status_code=status_code
            )

        db.close()

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
