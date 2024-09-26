import pyodbc

def get_top_5_errors():
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=10.103.34.23;DATABASE=TELCOPUSHPULL;UID=splunkpost;;PWD=Splunk@&*1!;')
    cursor = conn.cursor()

    query = """
    WITH ErrorCounts AS (
    SELECT 
        'MTN Pull' AS Service,
        CBS_ERROR_MESSAGE AS Error_Message,
        COUNT(*) AS Failure_Count,
        (SELECT COUNT(*) 
         FROM [TELCOPUSHPULL].[dbo].[TELCO_PULL_TRANS]
         WHERE CBS_STATUS NOT IN ('COMPLETED', 'PENDING')) AS Total_Failures
    FROM [TELCOPUSHPULL].[dbo].[TELCO_PULL_TRANS]
    WHERE CBS_STATUS NOT IN ('COMPLETED', 'PENDING')  -- Filter for failed transactions
      AND CBS_ERROR_MESSAGE IS NOT NULL  -- Exclude rows where CBS_ERROR_MESSAGE is NULL
    GROUP BY CBS_ERROR_MESSAGE
)
SELECT 
    Error_Message,
    Service,
    Failure_Count,
    CAST((CAST(Failure_Count AS DECIMAL(10,2)) / Total_Failures) * 100 AS DECIMAL(5,2)) AS Failure_Percentage
FROM ErrorCounts
ORDER BY Failure_Count DESC
OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY;  -- Fetch the top 5 errors

    """
    
    cursor.execute(query)
    result = cursor.fetchall()
    conn.close()

    return [{
        'error_message': row[0],
        'service': row[1],
        'failure_count': row[2],
        'failure_percentage': row[3]
    } for row in result]
