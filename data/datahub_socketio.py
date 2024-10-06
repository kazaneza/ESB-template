import pyodbc
import time
import threading
from flask_socketio import SocketIO

# Initialize SocketIO (will be set in app.py)
socketio = None

def fetch_and_emit_transactions():
    while True:
        try:
            conn = pyodbc.connect(
                'DRIVER={ODBC Driver 17 for SQL Server};'
                'SERVER=10.103.34.23;'
                'DATABASE=TELCOPUSHPULL;'
                'UID=splunkpost;'
                'PWD=Splunk@&*1!;'
            )
            cursor = conn.cursor()
            cursor.execute("""
                SELECT TOP 6 AMOUNT, TELCO, CBS_STATUS, TRANS_DT
                FROM [TELCOPUSHPULL].[dbo].[TELCO_PULL_TRANS]
                ORDER BY TRANS_DT DESC
            """)
            rows = cursor.fetchall()
            conn.close()

            transactions = [list(row) for row in rows]

            # Emit transactions to all connected clients
            if socketio:
                socketio.emit('update_transactions', transactions)
        except Exception as e:
            print(f"Error fetching transactions: {e}")

        time.sleep(1)  # Adjust the interval as needed

def start_datahub_thread(sio):
    global socketio
    socketio = sio  # Set the SocketIO instance

    update_thread = threading.Thread(target=fetch_and_emit_transactions)
    update_thread.daemon = True
    update_thread.start()
