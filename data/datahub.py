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
                WITH TELCO_PUSH AS (
    SELECT 
        COUNT(*) AS Total_Transactions,
        SUM(CASE WHEN CBS_STATUS = 'SUCCESS' THEN 1 ELSE 0 END) AS Completed_Transactions,
        SUM(CASE WHEN CBS_STATUS = 'PENDING' THEN 1 ELSE 0 END) AS Pending_Transactions,
        SUM(CASE WHEN CBS_STATUS NOT IN ('SUCCESS', 'PENDING') THEN 1 ELSE 0 END) AS Failed_Transactions,
        
        SUM(AMOUNT) AS Total_Amount,
        SUM(CASE WHEN CBS_STATUS = 'SUCCESS' THEN AMOUNT ELSE 0 END) AS Success_Amount,
        SUM(CASE WHEN CBS_STATUS = 'PENDING' THEN AMOUNT ELSE 0 END) AS Pending_Amount,
        SUM(CASE WHEN CBS_STATUS NOT IN ('SUCCESS', 'PENDING') THEN AMOUNT ELSE 0 END) AS Failed_Amount
    FROM TELCOPUSHPULL.dbo.TELCO_PUSH_TRANS
    WHERE TRY_CONVERT(DATETIMEOFFSET, TRANS_DT, 126) >= DATEADD(DAY, -150, SYSDATETIMEOFFSET())
),
TELCO_PULL AS (
    SELECT 
        COUNT(*) AS Total_Transactions,
        SUM(CASE WHEN CBS_STATUS = 'COMPLETED' THEN 1 ELSE 0 END) AS Completed_Transactions,
        SUM(CASE WHEN CBS_STATUS = 'PENDING' THEN 1 ELSE 0 END) AS Pending_Transactions,
        SUM(CASE WHEN CBS_STATUS NOT IN ('COMPLETED', 'PENDING') THEN 1 ELSE 0 END) AS Failed_Transactions,
        
        SUM(AMOUNT) AS Total_Amount,
        SUM(CASE WHEN CBS_STATUS = 'COMPLETED' THEN AMOUNT ELSE 0 END) AS Success_Amount,
        SUM(CASE WHEN CBS_STATUS = 'PENDING' THEN AMOUNT ELSE 0 END) AS Pending_Amount,
        SUM(CASE WHEN CBS_STATUS NOT IN ('COMPLETED', 'PENDING') THEN AMOUNT ELSE 0 END) AS Failed_Amount
    FROM TELCOPUSHPULL.dbo.TELCO_PULL_TRANS
    WHERE TRY_CONVERT(DATETIMEOFFSET, TRANS_DT, 126) >= DATEADD(DAY, -150, SYSDATETIMEOFFSET())
),
TAX_PAYMENT AS (
    SELECT 
        COUNT(*) AS Total_Transactions,
        SUM(CASE WHEN ftStatusCode = 'SUCCESS' THEN 1 ELSE 0 END) AS Completed_Transactions,
        SUM(CASE WHEN ftStatusCode = 'PENDING' THEN 1 ELSE 0 END) AS Pending_Transactions,
        SUM(CASE WHEN ftStatusCode NOT IN ('SUCCESS', 'PENDING') THEN 1 ELSE 0 END) AS Failed_Transactions,
        
        SUM(TRY_CONVERT(DECIMAL(18, 2), amount)) AS Total_Amount,
        SUM(CASE WHEN ftStatusCode = 'SUCCESS' THEN TRY_CONVERT(DECIMAL(18, 2), amount) ELSE 0 END) AS Success_Amount,
        SUM(CASE WHEN ftStatusCode = 'PENDING' THEN TRY_CONVERT(DECIMAL(18, 2), amount) ELSE 0 END) AS Pending_Amount,
        SUM(CASE WHEN ftStatusCode NOT IN ('SUCCESS', 'PENDING') THEN TRY_CONVERT(DECIMAL(18, 2), amount) ELSE 0 END) AS Failed_Amount
    FROM RRAACCOUNTS.dbo.TAX_PAYMENT_TRNX_MTN
    WHERE dbDateTime >= DATEADD(DAY, -150, GETDATE())
),
TELCO_IREMBO AS (
    SELECT 
        COUNT(*) AS Total_Transactions,
        SUM(CASE WHEN PAYMENT_STATUS = 'COMPLETED' THEN 1 ELSE 0 END) AS Completed_Transactions,
        SUM(CASE WHEN PAYMENT_STATUS = 'PENDING' THEN 1 ELSE 0 END) AS Pending_Transactions,
        SUM(CASE WHEN PAYMENT_STATUS NOT IN ('COMPLETED', 'PENDING') THEN 1 ELSE 0 END) AS Failed_Transactions,
        
        SUM(TRY_CONVERT(DECIMAL(18, 2), PAID_AMT)) AS Total_Amount,
        SUM(CASE WHEN PAYMENT_STATUS = 'COMPLETED' THEN TRY_CONVERT(DECIMAL(18, 2), PAID_AMT) ELSE 0 END) AS Success_Amount,
        SUM(CASE WHEN PAYMENT_STATUS = 'PENDING' THEN TRY_CONVERT(DECIMAL(18, 2), PAID_AMT) ELSE 0 END) AS Pending_Amount,
        SUM(CASE WHEN PAYMENT_STATUS NOT IN ('COMPLETED', 'PENDING') THEN TRY_CONVERT(DECIMAL(18, 2), PAID_AMT) ELSE 0 END) AS Failed_Amount
    FROM IREMBOGATEWAY.dbo.TELCO_IREMBO_TRANS
    WHERE TRY_CONVERT(DATETIMEOFFSET, PAYMENT_DT, 126) >= DATEADD(DAY, -150, SYSDATETIMEOFFSET())
),
EKash_transfer AS (
    SELECT 
        COUNT(*) AS Total_Transactions,
        SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS Completed_Transactions,
        SUM(CASE WHEN status = 'PENDING' THEN 1 ELSE 0 END) AS Pending_Transactions,
        SUM(CASE WHEN status NOT IN ('SUCCESS', 'PENDING') THEN 1 ELSE 0 END) AS Failed_Transactions,
        
        SUM(TRY_CONVERT(DECIMAL(18, 2), amount)) AS Total_Amount,
        SUM(CASE WHEN status = 'SUCCESS' THEN TRY_CONVERT(DECIMAL(18, 2), amount) ELSE 0 END) AS Success_Amount,
        SUM(CASE WHEN status = 'PENDING' THEN TRY_CONVERT(DECIMAL(18, 2), amount) ELSE 0 END) AS Pending_Amount,
        SUM(CASE WHEN status NOT IN ('SUCCESS', 'PENDING') THEN TRY_CONVERT(DECIMAL(18, 2), amount) ELSE 0 END) AS Failed_Amount
    FROM ESB_SERVICES.dbo.EKash_transfer
    WHERE TRY_CONVERT(DATETIMEOFFSET, createdAt, 126) >= DATEADD(DAY, -150, SYSDATETIMEOFFSET())
)

-- Combine results from all the services
SELECT
    -- Overall totals
    FORMAT(SUM(Total_Transactions), 'N0') AS Overall_Total_Transactions,
    FORMAT(SUM(Total_Amount), 'N2') AS Overall_Total_Amount,

    -- Success metrics
    FORMAT(SUM(Completed_Transactions), 'N0') AS Overall_Successful_Transactions,
    FORMAT(SUM(Success_Amount), 'N2') AS Overall_Successful_Amount,
    FORMAT(SUM(Completed_Transactions) * 1.0 / NULLIF(SUM(Total_Transactions), 0), 'P2') AS Overall_Success_Rate,

    -- Pending metrics
    FORMAT(SUM(Pending_Transactions), 'N0') AS Overall_Pending_Transactions,
    FORMAT(SUM(Pending_Amount), 'N2') AS Overall_Pending_Amount,
    FORMAT(SUM(Pending_Transactions) * 1.0 / NULLIF(SUM(Total_Transactions), 0), 'P2') AS Overall_Pending_Rate,

    -- Failed metrics
    FORMAT(SUM(Failed_Transactions), 'N0') AS Overall_Failed_Transactions,
    FORMAT(SUM(Failed_Amount), 'N2') AS Overall_Failed_Amount,
    FORMAT(SUM(Failed_Transactions) * 1.0 / NULLIF(SUM(Total_Transactions), 0), 'P2') AS Overall_Failure_Rate

FROM (
    SELECT * FROM TELCO_PUSH
    UNION ALL
    SELECT * FROM TELCO_PULL
    UNION ALL
    SELECT * FROM TAX_PAYMENT
    UNION ALL
    SELECT * FROM TELCO_IREMBO
    UNION ALL
    SELECT * FROM EKash_transfer
) AS CombinedResults;

            """,
            'database': 'ESB_SERVICES'
        }
    }

    # Initialize counters for overall metrics
    total_success_transactions = 0
    total_transactions = 0
    total_completed_amount = 0.0

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
            completed_transactions = result['Completed_Transactions'].iloc[0]
            total_transactions_service = result['Total_Transactions'].iloc[0]

            # Update cache for individual service
            cache[service_name] = {
                'success_rate': success_rate,
                'completed_transactions': completed_transactions,
                'moving_average_amount': total_amount
            }

            # Accumulate totals for all services
            total_success_transactions += completed_transactions
            total_transactions += total_transactions_service
            total_completed_amount += total_amount

        except Exception as e:
            print(f"Error processing {service_name}: {e}")
            continue  # Skip cache update if there's an error

        print(f"Cache updated at: {time.ctime()} for {service_name} with success rate: {cache[service_name]['success_rate']}% and moving average amount: {cache[service_name]['moving_average_amount']}")

    # Calculate overall success rate across all services
    overall_success_rate = (total_success_transactions / total_transactions * 100) if total_transactions > 0 else 0

    # Store overall metrics in cache
    cache['all_services'] = {
        'success_rate': overall_success_rate,
        'total_completed_transactions': total_success_transactions,
        'total_completed_amount': total_completed_amount
    }

    print(f"Overall success rate: {overall_success_rate}%, Total completed transactions: {total_success_transactions}, Total completed amount: {total_completed_amount}")

schedule.every(1).minutes.do(load_data_to_cache)

def start_cache_updater():
    load_data_to_cache()
    while True:
        schedule.run_pending()
        time.sleep(1)
