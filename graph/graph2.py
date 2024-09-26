import matplotlib
matplotlib.use('Agg') 

import matplotlib.pyplot as plt
import io
import base64
import pyodbc
import pandas as pd
from scipy.interpolate import make_interp_spline
import numpy as np

def create_graph_success():
    # Database connection string
    conn_str = (
        "Driver={ODBC Driver 17 for SQL Server};"
        "Server=10.103.34.23;"
        "UID=splunkpost;"
        "PWD=Splunk@&*1!;"
    )
    
    # SQL query to retrieve success transactions and moving average
    query = """
    WITH CombinedSuccess AS (
    -- TELCO_PUSH_TRANS: Success Status = 'SUCCESS'
    SELECT 
        DATEADD(hour, DATEDIFF(hour, 0, TRY_CONVERT(DATETIMEOFFSET, TRANS_DT, 126)), 0) AS Transaction_Hour
    FROM TELCOPUSHPULL.dbo.TELCO_PUSH_TRANS
    WHERE TRY_CONVERT(DATETIMEOFFSET, TRANS_DT, 126) >= DATEADD(hour, -6, SYSDATETIMEOFFSET())
        AND CBS_STATUS = 'SUCCESS'

    UNION ALL

    -- TELCO_PULL_TRANS: Success Status = 'COMPLETED'
    SELECT 
        DATEADD(hour, DATEDIFF(hour, 0, TRY_CONVERT(DATETIMEOFFSET, TRANS_DT, 126)), 0) AS Transaction_Hour
    FROM TELCOPUSHPULL.dbo.TELCO_PULL_TRANS
    WHERE TRY_CONVERT(DATETIMEOFFSET, TRANS_DT, 126) >= DATEADD(hour, -6, SYSDATETIMEOFFSET())
        AND CBS_STATUS = 'COMPLETED'

    UNION ALL

    -- TAX_PAYMENT_TRNX_MTN: Success Status = 'SUCCESS'
    SELECT 
        DATEADD(hour, DATEDIFF(hour, 0, TRY_CONVERT(DATETIMEOFFSET, dbDateTime, 126)), 0) AS Transaction_Hour
    FROM RRAACCOUNTS.dbo.TAX_PAYMENT_TRNX_MTN
    WHERE TRY_CONVERT(DATETIMEOFFSET, dbDateTime, 126) >= DATEADD(hour, -6, SYSDATETIMEOFFSET())
        AND ftStatusCode = 'SUCCESS'

    UNION ALL

    -- TELCO_IREMBO_TRANS: Success Status = 'COMPLETED'
    SELECT 
        DATEADD(hour, DATEDIFF(hour, 0, TRY_CONVERT(DATETIMEOFFSET, PAYMENT_DT, 126)), 0) AS Transaction_Hour
    FROM IREMBOGATEWAY.dbo.TELCO_IREMBO_TRANS
    WHERE TRY_CONVERT(DATETIMEOFFSET, PAYMENT_DT, 126) >= DATEADD(hour, -6, SYSDATETIMEOFFSET())
        AND PAYMENT_STATUS = 'COMPLETED'

    UNION ALL

    -- EKash_transfer: Success Status = 'SUCCESS'
    SELECT 
        DATEADD(hour, DATEDIFF(hour, 0, TRY_CONVERT(DATETIMEOFFSET, createdAt, 126)), 0) AS Transaction_Hour
    FROM ESB_SERVICES.dbo.EKash_transfer
    WHERE TRY_CONVERT(DATETIMEOFFSET, createdAt, 126) >= DATEADD(hour, -6, SYSDATETIMEOFFSET())
        AND status = 'SUCCESS'
),
HourlySuccess AS (
    SELECT
        Transaction_Hour,
        COUNT(*) AS Success_Count
    FROM CombinedSuccess
    GROUP BY Transaction_Hour
),
NumberedSuccess AS (
    SELECT
        Transaction_Hour,
        Success_Count,
        CAST(AVG(Success_Count) OVER (
            ORDER BY Transaction_Hour 
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) AS DECIMAL(10,2)) AS Moving_Average_Success,
        ROW_NUMBER() OVER (ORDER BY Transaction_Hour) AS RN
    FROM HourlySuccess
)

SELECT
    FORMAT(Transaction_Hour, 'h tt') AS Transaction_Hour,
    Success_Count,
    Moving_Average_Success
FROM NumberedSuccess
WHERE RN > 3  -- Exclude the first 3 hours
;


    """

    try:
        with pyodbc.connect(conn_str) as conn:
            df = pd.read_sql_query(query, conn)
    except pyodbc.Error as e:
        print("Error connecting to SQL Server:", e)
        return None

    if df.empty:
        print("No data retrieved from the database.")
        return None
    df['Transaction_Hour'] = pd.to_datetime(df['Transaction_Hour'], format='%I %p')
    
    df = df.sort_values(by='Transaction_Hour')

    df = df.reset_index(drop=True)
    df['Transaction_Hour'] = df['Transaction_Hour'].dt.strftime('%I %p')
    df['x_seq'] = df.index

    x = df['x_seq'].values
    y = df['Moving_Average_Success'].values

    if len(x) < 4:
        print("Not enough data points to perform smoothing.")
        smooth_y = y
        x_smooth = x
        x_labels = df['Transaction_Hour'].values
    else:
        try:
            spline = make_interp_spline(x, y, k=3)
            x_smooth = np.linspace(x.min(), x.max(), 300)
            smooth_y = spline(x_smooth)
        except Exception as e:
            print("Error during smoothing:", e)
            smooth_y = y
            x_smooth = x

        x_labels = df['Transaction_Hour'].values

    # Plotting
    fig, ax = plt.subplots(figsize=(10, 6))  

    if len(x) < 4:
        ax.plot(
            df['Transaction_Hour'], 
            df['Moving_Average_Success'], 
            color='white', 
            linewidth=2.5
        )
    else:
        ax.plot(
            x_smooth, 
            smooth_y, 
            color='white', 
            linewidth=2.5
        )

    ax.set_xlabel('Transaction Hour', color='white')
    ax.set_ylabel('Moving Average of Success', color='white')
    ax.set_title('Hourly Moving Average of Successful Transactions', color='white')

    ax.tick_params(axis='x', colors='white')
    ax.tick_params(axis='y', colors='white')

    ax.spines['top'].set_color('none')
    ax.spines['right'].set_color('none')
    ax.spines['left'].set_color('white')
    ax.spines['bottom'].set_color('white')

    ax.grid(color='white', linestyle='--', linewidth=0.5)

    fig.patch.set_alpha(0)
    ax.patch.set_alpha(0)

    if len(x) >= 4:
        ax.set_xticks(x)
        ax.set_xticklabels(x_labels, rotation=45, ha='right', color='white')
    else:
        plt.setp(ax.get_xticklabels(), rotation=45, ha='right', color='white')

    plt.tight_layout()

    img = io.BytesIO()
    fig.savefig(img, format='png', bbox_inches='tight', transparent=True)
    img.seek(0) 

    graph_url_success = base64.b64encode(img.getvalue()).decode('utf-8')
  
    plt.close(fig)

    return f"data:image/png;base64,{graph_url_success}"
