import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from graph1 import create_graph_failure

# Call the function to get the base64 image
graph_url_failure = create_graph_failure()

# Email details
sender_email = "dataanalytics@bk.rw"
receiver_email = "gkazaneza@bk.com"
password = "b9{OwX/^1[^8{rKs"  # Make sure the password is correct for Outlook

# Create a MIME multipart message
msg = MIMEMultipart('alternative')
msg['Subject'] = "Graph Failure Report"
msg['From'] = sender_email
msg['To'] = receiver_email

# HTML content with the embedded graph
html_content = f"""
<html>
  <body>
    <h1>Graph Failure Report</h1>
    <p>Here is the graph for the failed transactions:</p>
    <img src="{graph_url_failure}" alt="Graph Image" />
  </body>
</html>
"""

# Attach the HTML content to the email
msg.attach(MIMEText(html_content, 'html'))

# Set up the SMTP server for Outlook
try:
    server = smtplib.SMTP('smtp.office365.com', 587)  # SMTP server for Outlook
    server.starttls()  # Start TLS encryption
    server.login(sender_email, password)
    # Send the email
    server.sendmail(sender_email, receiver_email, msg.as_string())
    server.quit()
    print("Email sent successfully!")
except Exception as e:
    print(f"Failed to send email: {e}")
