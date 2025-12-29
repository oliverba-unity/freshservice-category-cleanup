import os
from dotenv import load_dotenv
from freshservice_api import FreshserviceApi

load_dotenv()

def main():
    fs = FreshserviceApi(
        api_key=os.getenv("FRESHSERVICE_API_KEY"),
        domain=os.getenv("FRESHSERVICE_API_DOMAIN")
    )

    try:
        ticket = fs.ticket(13).get()
        ticket.priority = 3
        ticket.update()
        print(f"Subject: {ticket.subject}")
        print(f"Priority: {ticket.priority}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()