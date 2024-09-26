import pandas as pd
from sqlalchemy import create_engine
import schedule
import time
 
cache = {}
moving_average_window = 7
 
connections = {
    'TELCOPUSHPULL': 'mssql+pyodbc://splunkpost:Splunk%40%26%2A1%21@10.103.34.23/TELCOPUSHPULL?driver=ODBC+Driver+17+for+SQL+Server',
    'RRAACCOUNTS': 'mssql+pyodbc://splunkpost:Splunk%40%26%2A1%21@10.103.34.23/RRAACCOUNTS?driver=ODBC+Driver+17+for+SQL+Server',
    'IREMBOGATEWAY': 'mssql+pyodbc://splunkpost:Splunk%40%26%2A1%21@10.103.34.23/IREMBOGATEWAY?driver=ODBC+Driver+17+for+SQL+Server',
    'ESB_SERVICES': 'mssql+pyodbc://splunkpost:Splunk%40%26%2A1%21@10.103.34.23/ESB_SERVICES?driver=ODBC+Driver+17+for+SQL+Server'
}
 
def load_data_to_cache():
    services = {
        'TELCO_PUSH_TRANS': {
            'query': """
                WITH last_7_days AS (
                    SELECT *
                    FROM TELCO_PUSH_TRANS
                    WHERE TRY_CONVERT(DATETIMEOFFSET, TRANS_DT, 126) >= DATEADD(DAY, -150, SYSDATETIMEOFFSET())
                )
                SELECT
                    COUNT(*) AS Total_Transactions,
                    SUM(CASE WHEN CBS_STATUS = 'SUCCESS' THEN 1 ELSE 0 END) AS Completed_Transactions,
                    ROUND((SUM(CASE WHEN CBS_STATUS = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0) / COUNT(*), 2) AS Success_Rate_Percentage,
                    ROUND(SUM(AMOUNT), 2) AS Total_Amount_Last_7_Days
                FROM last_7_days;
            """,
            'database': 'TELCOPUSHPULL'
        },
        'TELCO_PULL_TRANS': {
            'query': """
                WITH last_7_days AS (
                    SELECT *
                    FROM TELCO_PULL_TRANS
                    WHERE TRY_CONVERT(DATETIMEOFFSET, TRANS_DT, 126) >= DATEADD(DAY, -150, SYSDATETIMEOFFSET())
                )
                SELECT
                    COUNT(*) AS Total_Transactions,
                    SUM(CASE WHEN CBS_STATUS = 'COMPLETED' THEN 1 ELSE 0 END) AS Completed_Transactions,
                    ROUND((SUM(CASE WHEN CBS_STATUS = 'COMPLETED' THEN 1 ELSE 0 END) * 100.0) / COUNT(*), 2) AS Success_Rate_Percentage,
                    ROUND(SUM(AMOUNT), 2) AS Total_Amount_Last_7_Days
                FROM last_7_days;
            """,
            'database': 'TELCOPUSHPULL'
        },
        'TAX_PAYMENT_TRNX_MTN': {
            'query': """
                WITH last_7_days AS (
                    SELECT *
                    FROM TAX_PAYMENT_TRNX_MTN
                    WHERE dbDateTime >= DATEADD(DAY, -150, GETDATE())
                )
                SELECT
                    COUNT(*) AS Total_Transactions,
                    SUM(CASE WHEN ftStatusCode = 'SUCCESS' THEN 1 ELSE 0 END) AS Completed_Transactions,
                    ROUND((SUM(CASE WHEN ftStatusCode = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0) / COUNT(*), 2) AS Success_Rate_Percentage,
                    SUM(TRY_CONVERT(DECIMAL(18, 2), amount)) AS Total_Amount_Last_7_Days
                FROM last_7_days;
            """,
            'database': 'RRAACCOUNTS'
        },
        'TELCO_IREMBO_TRANS': {
            'query': """
                WITH last_7_days AS (
                    SELECT *
                    FROM TELCO_IREMBO_TRANS
                    WHERE TRY_CONVERT(DATETIMEOFFSET, PAYMENT_DT, 126) >= DATEADD(DAY, -150, SYSDATETIMEOFFSET())
                )
                SELECT
                    COUNT(*) AS Total_Transactions,
                    SUM(CASE WHEN PAYMENT_STATUS = 'COMPLETED' THEN 1 ELSE 0 END) AS Completed_Transactions,
                    ROUND((SUM(CASE WHEN PAYMENT_STATUS = 'COMPLETED' THEN 1 ELSE 0 END) * 100.0) / COUNT(*),2) AS Success_Rate_Percentage,
                    SUM(TRY_CONVERT(DECIMAL(18, 2), PAID_AMT)) AS Total_Amount_Last_7_Days
                FROM last_7_days;
            """,
            'database': 'IREMBOGATEWAY'
        },
        'AIRTEL_IREMBO_TRANS': {
            'query': """
                WITH last_7_days AS (
                    SELECT *
                    FROM AIRTEL_IREMBO_TRANS
                    WHERE TRY_CONVERT(DATETIMEOFFSET, PAYMENT_DT, 126) >= DATEADD(DAY, -150, SYSDATETIMEOFFSET())
                )
                SELECT
                    COUNT(*) AS Total_Transactions,
                    SUM(CASE WHEN PAYMENT_STATUS = 'COMPLETED' THEN 1 ELSE 0 END) AS Completed_Transactions,
                    ROUND((SUM(CASE WHEN PAYMENT_STATUS = 'COMPLETED' THEN 1 ELSE 0 END) * 100.0) / COUNT(*),2) AS Success_Rate_Percentage,
                    SUM(TRY_CONVERT(DECIMAL(18, 2), PAID_AMT)) AS Total_Amount_Last_7_Days
                FROM last_7_days;
            """,
            'database': 'IREMBOGATEWAY'
        },
        'EKash_transfer': {
            'query': """
                WITH last_7_days AS (
                    SELECT *
                    FROM EKash_transfer
                    WHERE TRY_CONVERT(DATETIMEOFFSET, createdAt, 126) >= DATEADD(DAY, -150, SYSDATETIMEOFFSET())
                )
                SELECT
                    COUNT(*) AS Total_Transactions,
                    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS Completed_Transactions,
                    ROUND((SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0) / COUNT(*), 2) AS Success_Rate_Percentage,
                    SUM(TRY_CONVERT(DECIMAL(18, 2), amount)) AS Total_Amount_Last_7_Days
                FROM last_7_days;
            """,
            'database': 'ESB_SERVICES'
        }
    }
 
    for service_name, config in services.items():
        query = config['query']
        database = config['database']
 
        try:
            print(f"Attempting to connect to {database} for service: {service_name}")
            engine = create_engine(connections[database])
 
            result = pd.read_sql(query, engine)
            print(f"Fetched success rate and amount for {service_name}")
 
            success_rate = result['Success_Rate_Percentage'].iloc[0]
            total_amount = result['Total_Amount_Last_7_Days'].iloc[0]
 
            cache[service_name] = {
                'success_rate': success_rate,
                'moving_average_amount': total_amount
            }
 
        except Exception as e:
            print(f"Error processing {service_name}: {e}")
            continue  # Skip cache update if there's an error
 
        print(f"Cache updated at: {time.ctime()} for {service_name} with success rate: {cache[service_name]['success_rate']}% and moving average amount: {cache[service_name]['moving_average_amount']}")
 
schedule.every(1).minutes.do(load_data_to_cache)
 
def start_cache_updater():
    load_data_to_cache()
    while True:
        schedule.run_pending()
        time.sleep(1)