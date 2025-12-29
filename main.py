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
        tickets_api = fs.ticket()
        ticket = tickets_api.get(1)
        print(f"Priority: {ticket.get('priority')}")

        tickets_api.update(1, {"priority": 1})

        ticket = tickets_api.get(1)
        print(f"Priority: {ticket.get('priority')}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()