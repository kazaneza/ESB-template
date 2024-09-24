import random
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import time

# Generate MTN Pull transaction data
def generate_mtn_pull_data(num_records=10):
    mtn_pull_data = []
    
    statuses = ['COMPLETED', 'FAILED']
    telco_statuses = ['SUBMITTED']
    cbs_error_messages = ['', 'Account has insufficient funds.']
    
    for _ in range(num_records):
        record = {
            'converted_time': (datetime.now() - timedelta(seconds=random.randint(0, 3600))).strftime('%d-%m-%Y %H:%M:%S'),
            'AMOUNT': round(random.uniform(100, 200000), 2),
            'CBS_STATUS': random.choice(statuses),
            'CBS_ERROR_MESSAGE': random.choice(cbs_error_messages),
            'TELCO': 'MTN',
            'TELCO_STATUS': random.choice(telco_statuses)
        }
        mtn_pull_data.append(record)
    
    return pd.DataFrame(mtn_pull_data)

# Generate MTN Push transaction data
def generate_mtn_push_data(num_records=10):
    mtn_push_data = []
    
    statuses = ['SUCCESS', 'PENDING']
    telco_statuses = ['COMPLETED', 'PENDING']
    
    for _ in range(num_records):
        record = {
            'converted_time': (datetime.now() - timedelta(seconds=random.randint(0, 3600))).strftime('%d-%m-%Y %H:%M:%S'),
            'AMOUNT': round(random.uniform(100, 1000000), 2),
            'CBS_STATUS': random.choice(statuses),
            'CBS_ERROR_MESSAGE': 'NULL',
            'TELCO': 'MTN',
            'TELCO_STATUS': random.choice(telco_statuses)
        }
        mtn_push_data.append(record)
    
    return pd.DataFrame(mtn_push_data)

# Insert the generated data into a SQL Server database
def insert_data_to_db(dataframe, table_name, db_url):
    engine = create_engine(db_url)
    dataframe.to_sql(table_name, con=engine, if_exists='append', index=False)

# Batch insertion with delay
def send_data_in_batches():
    # Database URL for SQL Server with Windows Authentication
    db_url = 'mssql+pyodbc://@ACADEMY06/esb?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes'
    
    # Insert data in small batches with a delay
    batch_size = 2  # Number of records per batch
    delay_seconds = 5  # Delay between each batch (in seconds)
    
    for _ in range(5):  # Adjust the number of iterations as needed
        # Generate a small batch of data
        df_mtn_pull = generate_mtn_pull_data(batch_size)
        df_mtn_push = generate_mtn_push_data(batch_size)
        
        # Insert the small batch of data into the respective tables
        insert_data_to_db(df_mtn_pull, 'mtn_pull', db_url)
        insert_data_to_db(df_mtn_push, 'mtn_push', db_url)
        
        print(f"Inserted {batch_size} records into mtn_pull and mtn_push tables.")
        
        # Wait before sending the next batch
        time.sleep(delay_seconds)

# Example usage
if __name__ == "__main__":
    send_data_in_batches()
