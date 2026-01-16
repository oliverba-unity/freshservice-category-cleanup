import datetime
import sqlite3
from typing import Optional

from freshservice_api.base_batch_processor import BaseBatchProcessor

class BatchTicketCategoryUpdater(BaseBatchProcessor):
    entity_label = "Ticket"

    def __init__(self, fs_api, db_filename):
        super().__init__(fs_api, db_filename=db_filename)

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
        with sqlite3.connect(self.db_filename) as db:
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

    def _fetch_and_lock_next_item(self, db):
        try:
            # Lock DB for this thread
            if self.random_order:
                next_ticket_query = """
                    SELECT *
                    FROM tickets
                    WHERE update_state = 'ready'
                    ORDER BY RANDOM()
                    LIMIT 1;
                """
            else:
                next_ticket_query = """
                    SELECT *
                    FROM tickets
                    WHERE update_state = 'ready'
                    ORDER BY id DESC
                    LIMIT 1;
                """
            db.execute("BEGIN IMMEDIATE")
            cursor = db.execute(next_ticket_query)

            ticket_row = cursor.fetchone()

            if not ticket_row:
                db.rollback()
                return None

            in_progress_query = """
                UPDATE tickets
                SET update_state      = 'in-progress',
                    request_timestamp = ?
                WHERE id = ?;
            """

            db.execute(in_progress_query, (datetime.datetime.now(), ticket_row['id']))
            db.commit()
            return ticket_row

        except sqlite3.OperationalError:
            db.rollback()
            return None

    def _perform_api_action(self, ticket_row):
        ticket_update_payload = {"category": ticket_row['new_category']}
        if ticket_row['new_sub_category']:
            ticket_update_payload["sub_category"] = ticket_row['new_sub_category']
        if ticket_row['new_item_category']:
            ticket_update_payload["item_category"] = ticket_row['new_item_category']

        return self.fs_api.ticket().update(ticket_row['id'], ticket_update_payload)

    def _handle_success(self, db, ticket_row, response):
        update_sql = """
            UPDATE tickets
            SET update_state         = 'updated',
                response_status_code = ?
            WHERE id = ?;
        """

        db.execute(update_sql, (response.status_code, ticket_row['id']))
        db.commit()

    def _handle_failure(self, db, ticket_row, status_code, error_message):
        update_sql = """
             UPDATE tickets
             SET update_state         = 'failed',
                 response_status_code = ?,
                 error_message        = ?
             WHERE id = ?; \
         """
        db.execute(update_sql, (status_code, error_message, ticket_row['id']))
        db.commit()
