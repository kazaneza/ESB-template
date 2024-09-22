import pandas as pd
from sqlalchemy import create_engine
import schedule
import time


data_hub = None

def load_data_to_hub():
    global data_hub
    engine = create_engine('mssql+pyodbc://@ACADEMY06/esb?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes')
    
    query = "SELECT * FROM mtn_pull"
    data_hub = pd.read_sql(query, engine)
    
    print("Data hub updated at:", time.ctime())

schedule.every(5).minutes.do(load_data_to_hub)

load_data_to_hub()

while True:
    schedule.run_pending()
    time.sleep(1)
