import os
import argparse
from dotenv import load_dotenv

from freshservice_api.freshservice_api import FreshserviceApi
from freshservice_api.batch_ticket_importer import BatchTicketImporter
from freshservice_api.batch_ticket_category_updater import BatchTicketCategoryUpdater

load_dotenv()

def main():
    parser = argparse.ArgumentParser(description="Freshservice Category Cleanup")
    parser.add_argument('action')
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
        "--limit",
        action="store",
        help="Limit the number of tickets to process"
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

    if args.action == "import-tickets":
        ticket_importer = BatchTicketImporter(fs_api=fs_api, db_filename=os.getenv("DB_FILENAME_IMPORT"))

        if args.create_tables:
            ticket_importer.create_tables()

        elif args.run:
            ticket_importer.run(limit=args.limit)

        elif args.retry_failed:
            ticket_importer.retry_failed()

        else:
            print("No arguments provided")

    elif args.action == "update-tickets":

        ticket_category_updater = BatchTicketCategoryUpdater(fs_api=fs_api, db_filename=os.getenv("DB_FILENAME_UPDATE"))

        if args.create_tables:
            ticket_category_updater.create_tables()

        elif args.prepare:
            ticket_category_updater.prepare()

        elif args.run:
            ticket_category_updater.run(limit=args.limit)

        elif args.retry_failed:
            ticket_category_updater.retry_failed()
        else:
            print("No arguments provided")
    else:
        print("Provide an action: import-tickets or update-tickets")
    exit()


if __name__ == "__main__":
    main()