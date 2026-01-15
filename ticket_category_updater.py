import datetime
import itertools
import time
import threading
import concurrent.futures
import sqlite3
from typing import Optional

from freshservice_api.freshservice_api import FreshserviceApi

print_lock = threading.Lock() # Prevent threads from simultaneously outputting to the console
db_lock = threading.Lock() # Prevent threads from simultaneously interacting with the database

class TicketCategoryUpdater(object):

    def __init__(self, fs_api: FreshserviceApi, db_filename: str="ticket_category_update.sqlite"):
        self.fs_api = fs_api
        self.db = sqlite3.connect(db_filename)
        self.db.row_factory = sqlite3.Row
        self.counter = None
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
        self.db.execute(create_tickets_table)
        self.db.execute(create_valid_categories_table)
        self.db.execute(create_category_mappings_table)

        print("Created tables.")

    def prepare(self):
        query = """
        SELECT * FROM tickets 
        WHERE update_state IS NULL
        ORDER BY id DESC 
        """

        tickets = self.db.execute(query).fetchall()

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
                self.db.execute(query, (ticket["id"],))
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
                    self.db.execute(
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
                    self.db.execute(query, (ticket["id"],))
                    unmapped = unmapped + 1

            total = total + 1

        self.db.commit()

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
        if category and sub_category and item_category:
            query = """
            SELECT * FROM valid_categories 
            WHERE category = ?
            AND sub_category = ?
            AND item_category = ?
            LIMIT 1;
            """
            cursor = self.db.execute(query, (category, sub_category, item_category))
        elif category and sub_category:
            query = """
            SELECT * FROM valid_categories 
            WHERE category = ?
            AND sub_category = ?
            AND item_category IS NULL
            LIMIT 1;
            """
            cursor = self.db.execute(query, (category, sub_category))
        elif category:
            query = """
            SELECT * FROM valid_categories 
            WHERE category = ?
            AND sub_category IS NULL
            AND item_category IS NULL
            LIMIT 1;
            """
            cursor = self.db.execute(query, (category,))
        else:
            return False

        result = cursor.fetchone()

        if result is not None:
            return True
        return False

    def get_new_category(self,
                         old_category: str,
                         old_sub_category: Optional[str] = None,
                         old_item_category: Optional[str] = None
                         ) -> Optional[sqlite3.Row]:

        if old_sub_category and old_item_category:
            query = """
                    SELECT * \
                    FROM category_mappings
                    WHERE old_category = ?
                      AND old_sub_category = ?
                      AND old_item_category = ?
                    LIMIT 1; \
                    """
            cursor = self.db.execute(query, (old_category, old_sub_category, old_item_category))
        elif old_sub_category:
            query = """
                    SELECT * \
                    FROM category_mappings
                    WHERE old_category = ?
                      AND old_sub_category = ?
                      AND old_item_category IS NULL
                    LIMIT 1; \
                    """
            cursor = self.db.execute(query, (old_category, old_sub_category))
        else:
            query = """
                    SELECT * \
                    FROM category_mappings
                    WHERE old_category = ?
                      AND old_sub_category IS NULL
                      AND old_item_category IS NULL
                    LIMIT 1; \
                    """
            cursor = self.db.execute(query, (old_category,))

        return cursor.fetchone()

    def run(self, max_workers: int=10):

        print(f"Starting ticket category updater with {max_workers} worker threads...")

        self.start_time = time.time()
        self.counter = itertools.count(start=1)

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

        final_count = next(self.counter) - 1

        print(f"Processed {final_count} tickets in {final_duration:.2f} seconds.")

        if final_duration > 0:
            print(f"Overall requests per minute: {(final_count / final_duration) * 60:.2f}")

    def retry_failed(self):
        query = """
        UPDATE tickets
        SET update_state = NULL,
            request_timestamp = NULL,
            response_status_code = NULL,
            error_message = NULL
        WHERE update_state = 'failed';
        """

        result = self.db.execute(query)
        quantity_to_retry = result.rowcount

        if quantity_to_retry > 0:
            print(f"Retrying updates for {quantity_to_retry} tickets...")
            self.run()
        else:
            print(f"No tickets to retry.")

    def _ticket_update_worker(self):
        while True:

            with db_lock:

                query = """
                        SELECT *
                        FROM tickets
                        WHERE update_state = 'ready'
                        ORDER BY id DESC
                        LIMIT 1;
                        """
                ticket = self.db.execute(query).fetchone()

                if not ticket:
                    # No more tickets left in the DB
                    break

                query = """
                        UPDATE tickets
                        SET update_state      = 'in-progress',
                            request_timestamp = ?
                        WHERE id = ?;
                        """

                self.db.execute(query, (datetime.datetime.now(), ticket['id']))

            update_payload = {"category": ticket['new_category']}
            if ticket['new_sub_category']:
                update_payload["sub_category"] = ticket['new_sub_category']
            if ticket['new_item_category']:
                update_payload["item_category"] = ticket['new_item_category']

            status_code = None

            try:
                response = self.fs_api.ticket().update(ticket['id'], update_payload)

                query = """
                        UPDATE tickets
                        SET update_state         = 'updated',
                            response_status_code = ?
                        WHERE id = ?;
                        """

                self.db.execute(query, (datetime.datetime.now(), response.status_code, ticket['id']))

            except Exception as e:
                error_message = str(e)

                if hasattr(e, 'response') and e.response is not None:
                    status_code = e.response.status_code
                    try:
                        # Try to get a clean error message from JSON
                        error_message = e.response.json()
                    except:
                        pass

                query = """
                        UPDATE tickets
                        SET update_state            = 'failed',
                            response_status_code    = ?,
                            error_message           = ?
                        WHERE id = ?;
                        """

                self.db.execute(
                    query,
                    (
                        datetime.datetime.now(),
                        status_code,
                        error_message,
                        ticket['id']
                    )
                )

            next(self.counter)

            self._print_progress(
                ticket_id=ticket['id'],
                status_code=status_code
            )

        self.db.close()

    def _print_progress(self, ticket_id: int, status_code: int | None = None):
        now = time.time()
        elapsed = now - self.start_time
        count = next(self.counter) - 1
        requests_per_minute = (count / elapsed) * 60 if elapsed > 0 else 0

        ratelimit_remaining = self.fs_api.controller.server_ratelimit_remaining,
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

        with print_lock:
            print(f"Ticket {ticket_id:06d} {icon} HTTP {status_code} "
                  f"[{bar}] Rate limit: {ratelimit_total} req/min. "
                  f"Remaining: {ratelimit_remaining:03d}. "
                  f"Current: {requests_per_minute:.1f} req/min")
