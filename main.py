import os
import argparse
from dotenv import load_dotenv

from freshservice_api.freshservice_api import FreshserviceApi
from ticket_category_updater import TicketCategoryUpdater

load_dotenv()

def main():
    parser = argparse.ArgumentParser(description="Freshservice Category Cleanup")
    parser.add_argument(
        "--create-tables",
        action="store_true",
        help="Create tables in database for ticket and category data"
    )

    parser.add_argument(
        "--prepare",
        action="store_true",
        help="Prepare tickets in the database with their new categories, ready for updating via API"
    )

    parser.add_argument(
        "--run",
        action="store_true",
        help="Update tickets in Freshservice using data from the database"
    )

    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="Retry tickets that failed in previous runs (HTTP code != 200)"
    )

    args = parser.parse_args()

    fs_api = FreshserviceApi(
        api_key=os.getenv("FRESHSERVICE_API_KEY"),
        domain=os.getenv("FRESHSERVICE_API_DOMAIN"),
        headroom=5
    )
    ticket_category_updater = TicketCategoryUpdater(fs_api)

    if args.create_tables:
        ticket_category_updater.create_tables()

    elif args.prepare:
        ticket_category_updater.prepare()

    elif args.run:
        ticket_category_updater.run()

    elif args.retry_failed:
        TicketCategoryUpdater(fs_api).retry_failed()
    else:
        print("No arguments provided")

if __name__ == "__main__":
    main()