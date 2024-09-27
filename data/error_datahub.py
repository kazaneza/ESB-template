import pyodbc

def get_top_5_errors():
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=10.103.34.23;DATABASE=TELCOPUSHPULL;UID=splunkpost;;PWD=Splunk@&*1!;')
    cursor = conn.cursor()

    query = """
     WITH ErrorCounts AS (
    SELECT 
        'MTN Pull' AS Service,
        CASE 
            WHEN CBS_ERROR_MESSAGE LIKE '%Account number Not found%' THEN 'Wrong Account Number'
            ELSE CBS_ERROR_MESSAGE
        END AS CBS_ERROR,
        COUNT(*) AS Failure_Count,
        (SELECT COUNT(*) 
         FROM [TELCOPUSHPULL].[dbo].[TELCO_PULL_TRANS]
         WHERE CBS_STATUS NOT IN ('COMPLETED', 'PENDING')) AS Total_Failures
    FROM [TELCOPUSHPULL].[dbo].[TELCO_PULL_TRANS]
    WHERE CBS_STATUS NOT IN ('COMPLETED', 'PENDING')  
      AND CBS_ERROR_MESSAGE IS NOT NULL  
    GROUP BY 
        CASE 
            WHEN CBS_ERROR_MESSAGE LIKE '%Account number Not found%' THEN 'Wrong Account Number'
            ELSE CBS_ERROR_MESSAGE
        END
)
SELECT 
    CBS_ERROR AS Error_Message,
    Service,
    Failure_Count,
    CAST((CAST(Failure_Count AS DECIMAL(10,2)) / Total_Failures) * 100 AS DECIMAL(5,2)) AS Failure_Percentage
FROM ErrorCounts
ORDER BY Failure_Count DESC
OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY;


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
