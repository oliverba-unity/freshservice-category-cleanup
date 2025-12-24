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
        # ticket = fs.ticket(13).get()
        # ticket.category = "Software"
        # ticket.sub_category = None
        # ticket.update()
        # pprint.pprint(ticket.to_dict())
        fs.ticket(13).update(payload=[["priority", 3]])
        # print(f"Subject: {ticket.subject}")
        # print(f"Status: {ticket.status}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()