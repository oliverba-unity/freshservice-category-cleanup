import datetime
import sqlite3

from freshservice_api.base_batch_processor import BaseBatchProcessor

class BatchTicketImporter(BaseBatchProcessor):
    entity_label = "Ticket"

    def __init__(self, fs_api, db_filename):
        super().__init__(fs_api, db_filename=db_filename)

    def create_tables(self):
        create_sql = """
             CREATE TABLE IF NOT EXISTS tickets
             (
                 id                   INTEGER PRIMARY KEY,
                 email                TEXT,
                 subject              TEXT,
                 description          TEXT,
                 category             TEXT,
                 sub_category         TEXT,
                 item_category        TEXT,
                 request_timestamp    TIMESTAMP DEFAULT NULL,
                 response_ticket_id   INTEGER,
                 response_status_code INTEGER   DEFAULT NULL,
                 error_message        TEXT      DEFAULT NULL
             );
        """
        with sqlite3.connect(self.db_filename) as db:
            db.execute(create_sql)
        print("Created tickets table.")

    def retry_failed(self):
        update_sql = """
            UPDATE tickets
            SET request_timestamp    = NULL,
                response_status_code = NULL,
                error_message        = NULL
            WHERE response_status_code IS NOT 201
              AND response_status_code IS NOT NULL;
            """
        with sqlite3.connect(self.db_filename) as db:
            result = db.execute(update_sql)
            db.commit()
            count = result.rowcount

        if count > 0:
            print(f"Retrying import of {count} tickets...")
            self.run()
        else:
            print("No tickets to retry.")

    def _fetch_and_lock_next_item(self, db):
        try:
            # Lock DB for this thread
            if self.random_order:
                next_ticket_query = """
                    SELECT *
                    FROM tickets
                    WHERE request_timestamp IS NULL
                    ORDER BY RANDOM()
                    LIMIT 1;
                """
            else:
                next_ticket_query = """
                    SELECT *
                    FROM tickets
                    WHERE request_timestamp IS NULL
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
                SET request_timestamp = ?
                WHERE id = ?;
            """

            db.execute(in_progress_query, (datetime.datetime.now(), ticket_row['id']))
            db.commit()
            return ticket_row

        except sqlite3.OperationalError:
            db.rollback()
            return None

    def _perform_api_action(self, ticket_row):
        payload = {
            "email": ticket_row["email"],
            "subject": ticket_row["subject"],
            "description": ticket_row["description"],
            "source": 1002, # API
            "category": ticket_row['category']
        }
        if ticket_row['sub_category']:
            payload["sub_category"] = ticket_row['sub_category']
        if ticket_row['item_category']:
            payload["item_category"] = ticket_row['item_category']

        return self.fs_api.ticket().create(payload)

    def _handle_success(self, db, ticket_row, response):
        response_json = response.json()
        response_ticket_id = response_json.get('ticket', {}).get('id')

        update_sql = """
            UPDATE tickets
            SET response_ticket_id   = ?,
                response_status_code = ?
            WHERE id = ?;
        """

        db.execute(update_sql, (response_ticket_id, response.status_code, ticket_row['id']))
        db.commit()

    def _handle_failure(self, db, ticket_row, status_code, error_message):
        update_sql = """
            UPDATE tickets
            SET response_status_code = ?,
                error_message        = ?
            WHERE id = ?;
        """
        db.execute(update_sql, (status_code, error_message, ticket_row['id']))
        db.commit()
