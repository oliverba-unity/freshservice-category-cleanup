import os
import time
import concurrent.futures
from dotenv import load_dotenv
from freshservice_api.freshservice_api import FreshserviceApi
from freshservice_api.exceptions import FreshserviceError

load_dotenv()

def main():
    fs = FreshserviceApi(
        api_key=os.getenv("FRESHSERVICE_API_KEY"),
        domain=os.getenv("FRESHSERVICE_API_DOMAIN"),
        headroom=5
    )

    ticket_service = fs.ticket()
    ticket_id = 1

    total_requests = 1000
    concurrency = 50

    start_time = time.time()
    completed_count = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:

        futures = {
            executor.submit(ticket_service.get, ticket_id): i
            for i in range(1, total_requests + 1)
        }

        for future in concurrent.futures.as_completed(futures):
            req_num = futures[future]
            completed_count += 1

            try:
                _ = future.result()

                remaining = fs.controller.server_ratelimit_remaining
                total = fs.controller.server_ratelimit_total

                now = time.time()
                elapsed = now - start_time
                if elapsed < 0.001: elapsed = 0.001  # Avoid zero division
                requests_per_minute = (completed_count / elapsed) * 60

                used = total - remaining
                percent_used = (used / total) * 100
                bar_len = 20
                filled = int(bar_len * percent_used / 100)
                bar = '█' * filled + '-' * (bar_len - filled)

                print(f"[{req_num:04d}/{total_requests}] ✅ Success. "
                      f"Quota: {remaining:04d}/{total} [{bar}] "
                      f"Requests per minute: {requests_per_minute:.1f}")

            except FreshserviceError as e:
                print(f"[{req_num:04d}] ❌ Failed: {e}")
            except Exception as e:
                print(f"[{req_num:04d}] 💥 Unexpected: {e}")

    final_duration = time.time() - start_time
    print(f"Overall requests per minute: {total_requests / (final_duration / 60):.2f}")

    fs.close()

if __name__ == "__main__":
    main()