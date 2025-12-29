import os
from dotenv import load_dotenv
from freshservice_api.freshservice_api import FreshserviceApi
from freshservice_api.exceptions import FreshserviceError

load_dotenv()

def main():
    fs = FreshserviceApi(
        api_key=os.getenv("FRESHSERVICE_API_KEY"),
        domain=os.getenv("FRESHSERVICE_API_DOMAIN")
    )

    ticket_service = fs.ticket()
    ticket_id = 1

    print(f"Starting rate limit test: Fetching Ticket #{ticket_id} 200 times...")

    try:
        for i in range(1, 201):
            ticket_service.get(ticket_id)

            # Print progress and current rate limit status
            print(f"[{i}/200] Fetch successful. "
                  f"Remaining Quota: {fs.rate_limit_remaining}/{fs.rate_limit_total}")

    except FreshserviceError as e:
        print(f"\nStopped early due to error: {e}")
    except KeyboardInterrupt:
        print("\nTest cancelled by user.")
    else:
        print("\nFinished 200 requests successfully!")

if __name__ == "__main__":
    main()