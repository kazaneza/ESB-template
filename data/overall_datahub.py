import pyodbc
import threading
import time

# Cache for overall metrics
overall_cache = {}

def fetch_overall_metrics():
    # Connection string to connect to your SQL Server
    conn_str = (
        "Driver={ODBC Driver 17 for SQL Server};"
        "Server=10.103.34.23;"
        "UID=splunkpost;"
        "PWD=Splunk@&*1!;"
    )

    # Your SQL query with explicit database references
    sql_query = """
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
    WHERE CAST(TRY_CONVERT(DATETIMEOFFSET, TRANS_DT, 126) AS DATE) = CAST(SYSDATETIMEOFFSET() AS DATE)

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
    WHERE CAST(TRY_CONVERT(DATETIMEOFFSET, TRANS_DT, 126) AS DATE) = CAST(SYSDATETIMEOFFSET() AS DATE)
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
    WHERE CAST(TRY_CONVERT(DATETIMEOFFSET, dbDateTime, 126) AS DATE) = CAST(SYSDATETIMEOFFSET() AS DATE)
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
    WHERE CAST(TRY_CONVERT(DATETIMEOFFSET, PAYMENT_DT, 126) AS DATE) = CAST(SYSDATETIMEOFFSET() AS DATE)
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
    WHERE CAST(TRY_CONVERT(DATETIMEOFFSET, createdAt, 126) AS DATE) = CAST(SYSDATETIMEOFFSET() AS DATE)
)

-- Combine results from all the services
SELECT
    -- Overall totals
    FORMAT(SUM(Total_Transactions), 'N0') AS Overall_Total_Transactions,
    FORMAT(SUM(Total_Amount), 'N2') AS Overall_Total_Amount,

    -- Success metrics
    FORMAT(SUM(Completed_Transactions), 'N0') AS Overall_Successful_Transactions,
    FORMAT(SUM(Success_Amount), 'N2') AS Overall_Successful_Amount,
    FORMAT(ROUND(SUM(Completed_Transactions) * 100.0 / NULLIF(SUM(Total_Transactions), 0), 2), 'N2') + '%' AS Overall_Success_Rate,

    -- Pending metrics
    FORMAT(SUM(Pending_Transactions), 'N0') AS Overall_Pending_Transactions,
    FORMAT(SUM(Pending_Amount), 'N2') AS Overall_Pending_Amount,
    FORMAT(ROUND(SUM(Pending_Transactions) * 100.0 / NULLIF(SUM(Total_Transactions), 0), 2), 'N2') + '%' AS Overall_Pending_Rate,

    -- Failed metrics
    FORMAT(SUM(Failed_Transactions), 'N0') AS Overall_Failed_Transactions,
    FORMAT(SUM(Failed_Amount), 'N2') AS Overall_Failed_Amount,
    FORMAT(ROUND(SUM(Failed_Transactions) * 100.0 / NULLIF(SUM(Total_Transactions), 0), 2), 'N2') + '%' AS Overall_Failure_Rate

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



    """

    try:
        # Connect to the database
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Execute the query
        cursor.execute(sql_query)

        # Fetch the result
        row = cursor.fetchone()

        # Map the result to a dictionary
        metrics = {
            'Overall_Total_Transactions': row.Overall_Total_Transactions,
            'Overall_Total_Amount': row.Overall_Total_Amount,
            'Overall_Successful_Transactions': row.Overall_Successful_Transactions,
            'Overall_Successful_Amount': row.Overall_Successful_Amount,
            'Overall_Success_Rate': row.Overall_Success_Rate,
            'Overall_Pending_Transactions': row.Overall_Pending_Transactions,
            'Overall_Pending_Amount': row.Overall_Pending_Amount,
            'Overall_Pending_Rate': row.Overall_Pending_Rate,
            'Overall_Failed_Transactions': row.Overall_Failed_Transactions,
            'Overall_Failed_Amount': row.Overall_Failed_Amount,
            'Overall_Failure_Rate': row.Overall_Failure_Rate,
        }

        # Update the cache
        overall_cache.update(metrics)

        # Close the connection
        cursor.close()
        conn.close()

        print("Fetched overall metrics and updated overall_cache.")
    except Exception as e:
        print(f"Error fetching overall metrics: {e}")

def start_overall_metrics_updater(interval=300):
    def run():
        while True:
            fetch_overall_metrics()
            time.sleep(interval)  # Update every 5 minutes

    updater_thread = threading.Thread(target=run)
    updater_thread.daemon = True
    updater_thread.start()
