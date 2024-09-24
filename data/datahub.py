import pandas as pd
from sqlalchemy import create_engine
import schedule
import time

cache = {}

# Connection strings for different databases
connections = {
    'TELCOPUSHPULL': 'mssql+pyodbc://@lsttest/TELCOPUSHPULL?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes',
    'RRAACCOUNTS': 'mssql+pyodbc://@lsttest/RRAACCOUNTS?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes',
    'IREMBOGATEWAY': 'mssql+pyodbc://@lsttest/IREMBOGATEWAY?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes',
    'ESB_SERVICES': 'mssql+pyodbc://@lsttest/ESB_SERVICES?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes'
}

def load_data_to_cache():
    # Define queries, status columns, and databases for each service
    services = {
        'TELCO_PUSH_TRANS': {
            'query': "SELECT * FROM TELCO_PUSH_TRANS",
            'status_column': 'CBS_STATUS',
            'completed_value': 'SUCCESS',
            'database': 'TELCOPUSHPULL'
        },
        'TELCO_PULL_TRANS': {
            'query': "SELECT * FROM TELCO_PULL_TRANS",
            'status_column': 'CBS_STATUS',
            'completed_value': 'COMPLETED',
            'database': 'TELCOPUSHPULL'
        },
        'TAX_PAYMENT_TRNX_MTN': {
            'query': "SELECT * FROM TAX_PAYMENT_TRNX_MTN",
            'status_column': 'ftStatusCode',
            'completed_value': 'SUCCESS',
            'database': 'RRAACCOUNTS'
        },
        'TELCO_IREMBO_TRANS': {
            'query': "SELECT * FROM TELCO_IREMBO_TRANS",
            'status_column': 'CBS_STATUS',
            'completed_value': 'CBS_SUCCESSFUL',
            'database': 'IREMBOGATEWAY'
        },
        'AIRTEL_IREMBO_TRANS': {
            'query': "SELECT * FROM AIRTEL_IREMBO_TRANS",
            'status_column': 'CBS_STATUS',
            'completed_value': 'CBS_SUCCESSFUL',
            'database': 'IREMBOGATEWAY'
        },
        'EKash_transfer': {
            'query': "SELECT * FROM EKash_transfer",
            'status_column': 'EKASHSTATUS',
            'completed_value': 'SUCCESS',
            'database': 'ESB_SERVICES'
        }
    }

    for service_name, config in services.items():
        query = config['query']
        status_column = config['status_column']
        completed_value = config['completed_value']
        database = config['database']

        try:
            # Create engine for the specific database
            engine = create_engine(connections[database])

            # Fetch data using the respective connection
            data = pd.read_sql(query, engine)
            total_rows = len(data)
            completed_rows = len(data[data[status_column] == completed_value])

            # Calculate success rate or set to 100% if no data
            # Calculate success rate or set to 100% if no data
            success_rate = round((completed_rows / total_rows) * 100, 2) if total_rows > 0 else 100.00


        except Exception as e:
            print(f"Error processing {service_name}: {e}")
            success_rate = 100  # Default to 100% if there's any issue or no data

        # Cache the success rate
        cache[f'{service_name}_success_rate'] = {
            'success_rate': success_rate,
            'last_updated': time.ctime()
        }

        print(f"Cache updated at: {time.ctime()} for {service_name} with success rate: {success_rate}%")

schedule.every(5).minutes.do(load_data_to_cache)

def start_cache_updater():
    load_data_to_cache()  # Initial run
    while True:
        schedule.run_pending()
        time.sleep(1)
