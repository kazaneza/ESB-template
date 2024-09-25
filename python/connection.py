import pyodbc
import logging

# Configure logging
logging.basicConfig(
    filename='db_connection.log',  
    level=logging.DEBUG,           
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Connection string
conn_str = (
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=10.103.34.23;'
    'DATABASE=TELCOPUSHPULL;'
    'UID=splunkpost;'
    'PWD={Splunk@&*1!};'
    'Encrypt=Yes;'
    'TrustServerCertificate=Yes;'
)




try:
    logging.info("Attempting to connect to the database...")
    conn = pyodbc.connect(conn_str, timeout=10)  
    logging.info("Connected to the database successfully!")
    print("Connected to the database successfully!")
    conn.close()
except pyodbc.Error as ex:
    logging.error("Failed to connect to the database.", exc_info=True)
    print("Failed to connect to the database.")
    print(ex)
