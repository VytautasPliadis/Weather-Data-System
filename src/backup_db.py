#!/usr/bin/env python3
import json
import os

from dotenv import load_dotenv
from supabase import create_client, Client

# Environment variables
load_dotenv()
KEY = os.getenv('API_KEY')
URL = os.getenv('PROJECT_URL')
TABLE_NAME = os.getenv('TABLE_NAME')
FILE_PATH = os.getenv('file_path')
supabase = create_client(URL, KEY)


def backup_table_to_json(table_name: str, file_path: str) -> None:
    """
    Backs up the specified Supabase table to a JSON file.

    This function queries all records from the specified table in a Supabase database and saves the data to a file in
    JSON format. If the table is successfully backed up, it prints a confirmation message. If the backup fails, it prints an error message.

    Args:
        table_name (str): The name of the table in the Supabase database to back up.
        file_path (str): The file path where the backup JSON file will be saved.

    Returns:
        None
    """
    data = supabase.table(table_name).select("*").execute()
    if data.data:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data.data, f, ensure_ascii=False, indent=4)
        print(f"Backup of table '{table_name}' completed successfully.")
    else:
        print(f"Failed to backup table '{table_name}'. Error: {data.error}")


backup_table_to_json(TABLE_NAME, FILE_PATH)
