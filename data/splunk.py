import splunklib.client as client
import splunklib.results as results

# Splunk connection details
SPLUNK_HOST = 'splunk1.bk.rw'  # No 'https://'
SPLUNK_PORT = 8089  # Use the API port (usually 8089)
USERNAME = 'gkazaneza'
PASSWORD = '25809Rushimisha!'
APP = 'Splunk BK Monitoring App'

# Initialize Splunk connection
service = client.connect(
    host=SPLUNK_HOST,
    port=SPLUNK_PORT,
    username=USERNAME,
    password=PASSWORD,
    scheme='https',  # Use https if required
    app=APP
)

# Splunk query
query = """
index=esb sourcetype=TELCO_PUSH_TRANS 
| stats count as TOTAL
| appendcols [search index=esb sourcetype=TELCO_PUSH_TRANS CBS_STATUS=SUCCESS | stats count as TOTAL_SUCCESS]
| eval success_rate = (TOTAL_SUCCESS/TOTAL) * 100 
| fields - TOTAL - TOTAL_SUCCESS
"""

# Run the query
job = service.jobs.create(query, exec_mode='blocking')

# Get results
results_reader = results.ResultsReader(job.results())

# Process the results
for result in results_reader:
    if isinstance(result, dict):
        success_rate = result.get('success_rate', 'N/A')
        print(f"Push Success Rate: {success_rate}%")
