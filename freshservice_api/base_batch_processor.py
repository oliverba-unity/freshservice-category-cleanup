import json
import time
import threading
import concurrent.futures
import sqlite3
from abc import ABC, abstractmethod
from typing import Optional

from freshservice_api import FreshserviceApi

class BaseBatchProcessor(ABC):
    entity_label = "Entity"

    def __init__(self, fs_api: FreshserviceApi, db_filename: str):
        self.fs_api = fs_api
        self.db_filename = db_filename
        self.iteration_count = 0
        self.iteration_limit = None
        self.success_count = 0
        self.failure_count = 0
        self.start_time = None
        self.count_lock = threading.Lock()
        self.print_lock = threading.Lock()

    def run(self, limit: int = None, max_workers: int = 10):
        print(f"Starting {self.__class__.__name__} with {max_workers} threads...")
        self.start_time = time.time()
        self.iteration_count = 0

        if limit:
            self.iteration_limit = int(limit)
            print(f"Limit set to {limit} {self.entity_label.lower()}s.")

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for i in range(max_workers):
                futures.append(
                    executor.submit(self._worker_loop)
                )

            # As futures finish, return their result, to re-raise and catch any exceptions
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Worker thread failed: {e}")

        self._print_final_stats()

    def _worker_loop(self):
        with sqlite3.connect(self.db_filename, timeout=30.0) as db:
            db.row_factory = sqlite3.Row

            while True:
                with self.count_lock:
                    if self.iteration_limit and self.iteration_count >= self.iteration_limit:
                        break
                    self.iteration_count += 1

                ticket_row = self._fetch_and_lock_next_item(db)
                if not ticket_row:
                    break

                try:
                    response = self._perform_api_action(ticket_row)

                    status_code = response.status_code

                    self._handle_success(db, ticket_row, response)

                    with self.count_lock:
                        self.success_count += 1

                except Exception as e:
                    with self.count_lock:
                        self.failure_count += 1

                    status_code = getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None

                    error_message = str(e)

                    if hasattr(e, 'response') and e.response is not None:
                        status_code = e.response.status_code
                        try:
                            error_message = json.dumps(e.response.json())
                        except:
                            error_message = e.response.text

                    self._handle_failure(db, ticket_row, status_code, error_message)

                # 4. Update Console
                self._print_progress(ticket_row['id'], status_code)

    def _print_progress(self, row_id: int, status_code: Optional[int]):
        now = time.time()
        elapsed = now - self.start_time if self.start_time else 0
        total_count = self.success_count + self.failure_count
        requests_per_minute = (total_count / elapsed) * 60 if elapsed > 0 else 0

        # API Rate Limit Tracking
        ratelimit_remaining = self.fs_api.controller.server_ratelimit_remaining
        ratelimit_total = self.fs_api.controller.server_ratelimit_total
        used = ratelimit_total - ratelimit_remaining

        pct = (used / ratelimit_total) * 100 if ratelimit_total > 0 else 0
        bar_len = 15
        filled = int(bar_len * pct / 100)
        bar = '█' * filled + '-' * (bar_len - filled)

        icon = "❌"
        if status_code and 200 <= status_code < 300:
            icon = "✅"
        elif status_code == 429:
            icon = "🚦"

        with self.print_lock:
            print(f"{self.entity_label} {row_id:06d} {icon} HTTP {status_code or '???'} "
                  f"[{bar}] Rate limit: {ratelimit_total} req/min. "
                  f"Current: {requests_per_minute:.1f} req/min. "
                  f"Remaining: {ratelimit_remaining:03d}.")

    def _print_final_stats(self):
        finish_time = time.time()
        duration = finish_time - self.start_time if self.start_time else 0
        total = self.success_count + self.failure_count

        print(f"Processed {total} {self.entity_label.lower()}s in {duration:.2f} seconds.")
        print(f"{self.failure_count} failed.")
        print(f"{self.success_count} successful.")

        if duration > 0:
            print(f"Overall requests per minute: {(total / duration) * 60:.2f}.")

    @abstractmethod
    def create_tables(self):
        pass

    @abstractmethod
    def retry_failed(self):
        pass

    @abstractmethod
    def _fetch_and_lock_next_item(self, db_conn: sqlite3.Connection) -> Optional[sqlite3.Row]:
        pass

    @abstractmethod
    def _perform_api_action(self, row: sqlite3.Row):
        pass

    @abstractmethod
    def _handle_success(self, db_conn: sqlite3.Connection, row: sqlite3.Row, response):
        pass

    @abstractmethod
    def _handle_failure(self, db_conn: sqlite3.Connection, ticket_row: sqlite3.Row, status_code: int, error_message: str):
        pass