import plotly.graph_objects as go

labels = ['January', 'February', 'March', 'April', 'May', 'June', 'July']
data = [12, 19, 3, 5, 2, 3, 7]

fig = go.Figure()

# Create a bar chart
fig.add_trace(go.Bar(
    x=labels,
    y=data,
    marker=dict(color='green'),
    text=data,
    textposition='auto',
))

# Customize the chart's layout
fig.update_layout(
    title="Total Successful Transactions",
    xaxis_title="Month",
    yaxis_title="Transactions",
    height=170,  # Adjusting height to match the card's container
    margin=dict(l=20, r=20, t=40, b=20)  # Adjusting margins
)

# Export the figure to an HTML string
html_chart = fig.to_html(full_html=False, include_plotlyjs='cdn')
