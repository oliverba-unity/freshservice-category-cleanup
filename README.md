# freshservice-api-tools

Tools for working with Freshservice via its API.

All actions are thread-safe and attempt to avoid exceeding Freshservice's rate limits, even when other systems are also
interfacing with Freshservice's API. When rate limited, retry requests up to 5 times.

## Setup

1. `git clone git@github.com:oliverba-unity/freshservice-api-tools.git`
2. `python3 -m venv .venv`
3. Activate the virtual environment
   1. Bash: `source ./venv/bin/activate`
   2. Fish: `source .venv/bin/activate.fish`
4. `pip install --requirement requirements.txt`
5. `cp .env.example .env`
6. `nano .env`

# Usage

## Batch import tickets

1. `python3 main.py import-tickets --create-tables`
2. Import your CSV of tickets into the `tickets` table in `import.sqlite`
3. `python3 main.py import-tickets --run`
4. Review the data in the `tickets` table to see error messages

## Batch update ticket categories
1. `python3 main.py update-tickets --create-tables`
2. Import your CSVs into `update.sqlite`:
   1. Import valid categories into the `valid_categories` table
   2. Import mappings of old categories to new categories into the `category_mappings` table
   3. Import tickets to update into the `tickets` table
3. `python3 main.py import-tickets --prepare`
4. Review the data in the `tickets` table
5. `python3 main.py import-tickets --run`
6. Review the data in the `tickets` table to see error messages

## Global options:
* `--limit N` - process N items and then exit 
* `--random-order` - process items in random order, instead of sequentially
* `--retry-failed` - clear all error messages from the database and retry the actions
