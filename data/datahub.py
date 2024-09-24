import pandas as pd
from sqlalchemy import create_engine
import schedule
import time
from collections import deque  # To store recent amounts for moving averages

cache = {}
moving_average_window = 7  # Moving average over last 7 records

# Connection strings for different databases
connections = {
    'TELCOPUSHPULL': 'mssql+pyodbc://@lsttest/TELCOPUSHPULL?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes',
    'RRAACCOUNTS': 'mssql+pyodbc://@lsttest/RRAACCOUNTS?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes',
    'IREMBOGATEWAY': 'mssql+pyodbc://@lsttest/IREMBOGATEWAY?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes',
    'ESB_SERVICES': 'mssql+pyodbc://@lsttest/ESB_SERVICES?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes'
}

# Initialize the cache with deques to store recent amounts for moving averages
for service_name in ['TELCO_PUSH_TRANS', 'TELCO_PULL_TRANS', 'TAX_PAYMENT_TRNX_MTN', 'TELCO_IREMBO_TRANS', 'AIRTEL_IREMBO_TRANS', 'EKash_transfer']:
    cache[service_name] = {
        'success_rates': deque(maxlen=moving_average_window),  # Store success rates
        'moving_averages': deque(maxlen=moving_average_window),  # Store up to 7 recent amounts for moving averages
        'moving_average_amount': 0.0,
        'success_rate': 100.0  # Default to 100%
    }

def calculate_moving_average(service_name):
    amounts = cache[service_name]['moving_averages']
    if len(amounts) == 0:
        return 0.0
    return round(sum(amounts) / len(amounts), 2)

def load_data_to_cache():
    services = {
        'TELCO_PUSH_TRANS': {
            'query': "SELECT * FROM TELCO_PUSH_TRANS",
            'amount_column': 'AMOUNT_LCY',  # Column for amount
            'time_column': 'TRANSACTION_DATE',  # Column for time
            'status_column': 'CBS_STATUS',
            'completed_value': 'SUCCESS',
            'database': 'TELCOPUSHPULL'
        },
        'TELCO_PULL_TRANS': {
            'query': "SELECT * FROM TELCO_PULL_TRANS",
            'amount_column': 'AMOUNT_FCY',  # Column for amount
            'time_column': 'TXN_DATE',  # Column for time
            'status_column': 'CBS_STATUS',
            'completed_value': 'COMPLETED',
            'database': 'TELCOPUSHPULL'
        },
        'TAX_PAYMENT_TRNX_MTN': {
            'query': "SELECT * FROM TAX_PAYMENT_TRNX_MTN",
            'amount_column': 'TOTAL_AMOUNT',  # Column for amount
            'time_column': 'PAYMENT_DATE',  # Column for time
            'status_column': 'ftStatusCode',
            'completed_value': 'SUCCESS',
            'database': 'RRAACCOUNTS'
        },
        'TELCO_IREMBO_TRANS': {
            'query': "SELECT * FROM TELCO_IREMBO_TRANS",
            'amount_column': 'PAYMENT_AMOUNT',  # Column for amount
            'time_column': 'PAYMENT_DATE',  # Column for time
            'status_column': 'CBS_STATUS',
            'completed_value': 'CBS_SUCCESSFUL',
            'database': 'IREMBOGATEWAY'
        },
        'AIRTEL_IREMBO_TRANS': {
            'query': "SELECT * FROM AIRTEL_IREMBO_TRANS",
            'amount_column': 'TRANSFER_AMOUNT',  # Column for amount
            'time_column': 'DATE_TIME',  # Column for time
            'status_column': 'CBS_STATUS',
            'completed_value': 'CBS_SUCCESSFUL',
            'database': 'IREMBOGATEWAY'
        },
        'EKash_transfer': {
            'query': "SELECT * FROM EKash_transfer",
            'amount_column': 'AMOUNT_TRANSFERRED',  # Column for amount
            'time_column': 'TRANSFER_DATE',  # Column for time
            'status_column': 'EKASHSTATUS',
            'completed_value': 'SUCCESS',
            'database': 'ESB_SERVICES'
        }
    }

    for service_name, config in services.items():
        query = config['query']
        amount_column = config['amount_column']
        time_column = config['time_column']
        status_column = config['status_column']
        completed_value = config['completed_value']
        database = config['database']

        try:
            # Create engine for the specific database
            engine = create_engine(connections[database])

            # Fetch data using the respective connection
            data = pd.read_sql(query, engine)

            # Filter by the last 7 days based on the time column
            data[time_column] = pd.to_datetime(data[time_column])  # Ensure time column is datetime
            last_7_days = data[data[time_column] >= (pd.Timestamp.now() - pd.Timedelta(days=7))]

            # Calculate success rate
            total_rows = len(data)
            completed_rows = len(data[data[status_column] == completed_value])
            success_rate = round((completed_rows / total_rows) * 100, 2) if total_rows > 0 else 100.00

            # Calculate total amount for the last 7 days
            total_amount = last_7_days[amount_column].sum()

            # Append the amount and success rate to the deque and update the moving average
            cache[service_name]['moving_averages'].append(total_amount)
            cache[service_name]['success_rates'].append(success_rate)
            cache[service_name]['moving_average_amount'] = calculate_moving_average(service_name)
            cache[service_name]['success_rate'] = success_rate

        except Exception as e:
            print(f"Error processing {service_name}: {e}")
            cache[service_name]['moving_average_amount'] = 0.0  # Default to 0 if there's any issue
            cache[service_name]['success_rate'] = 100.0  # Default to 100%

        print(f"Cache updated at: {time.ctime()} for {service_name} with success rate: {cache[service_name]['success_rate']}% and moving average amount: {cache[service_name]['moving_average_amount']}")

schedule.every(5).minutes.do(load_data_to_cache)

def start_cache_updater():
    load_data_to_cache()  # Initial run
    while True:
        schedule.run_pending()
        time.sleep(1)
