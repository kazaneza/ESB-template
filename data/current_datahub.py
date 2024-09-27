from datetime import datetime
import time
import threading
import pyodbc

# Cache to store the latest 10 transactions
latest_transactions = []

def fetch_latest_transactions():
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=10.103.34.23;'
        'DATABASE=TELCOPUSHPULL;'
        'UID=splunkpost;'
        'PWD=Splunk@&*1!;'
    )
    cursor = conn.cursor()
    cursor.execute("""
        SELECT TOP 10 AMOUNT, TELCO, CBS_STATUS, TRANS_DT
        FROM [TELCOPUSHPULL].[dbo].[TELCO_PULL_TRANS]
        ORDER BY TRANS_DT DESC
    """)
    rows = cursor.fetchall()
    conn.close()
    
    # Convert each row to a list
    transactions = [list(row) for row in rows]
    return transactions


def update_cache():
    global latest_transactions
    while True:
        try:
            latest_transactions = fetch_latest_transactions()
        except Exception as e:
            print(f"Error updating cache: {e}")
        time.sleep(1)


def get_cached_transactions():
    return latest_transactions

update_thread = threading.Thread(target=update_cache, daemon=True)
update_thread.start()