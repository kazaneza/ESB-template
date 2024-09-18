from flask import Flask, render_template
import plotly.graph_objects as go

app = Flask(__name__)

# Define the route and logic for rendering the dashboard
@app.route('/')
def dashboard():
    # Define the data for the Plotly chart
    months = ['Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    values = [50, 40, 300, 320, 500, 350, 200, 230, 500]

    # Create the Plotly figure
    fig = go.Figure()

    # Add a line trace to the figure
    fig.add_trace(go.Scatter(
        x=months, 
        y=values, 
        mode='lines+markers',
        line=dict(color='white', width=3),  # White line
        marker=dict(color='white', size=8),  # White circular markers
    ))

    # Update the layout for a green background
    fig.update_layout(
        xaxis_title="",  # Empty x-axis title
        yaxis_title="",  # Empty y-axis title
        plot_bgcolor='green',  # Green background color
        paper_bgcolor='green',  # Green background for the entire chart
        showlegend=False,  # Hide legend
        margin=dict(l=20, r=20, t=20, b=20),  # Small margins
    )

    # Update x and y axis properties to match the design
    fig.update_xaxes(
        showgrid=False,  # Hide vertical grid lines
        tickcolor="white",  # White ticks on the x-axis
        tickfont=dict(color="white"),  # White labels on the x-axis
    )
    fig.update_yaxes(
        showgrid=True,  # Keep horizontal grid lines
        gridcolor="rgba(255,255,255,0.2)",  # Light white grid lines
        zeroline=False,  # Hide the zero line
        tickcolor="white",  # White ticks on the y-axis
        tickfont=dict(color="white"),  # White labels on the y-axis
    )

    # Convert the Plotly figure to an HTML string
    chart_html = fig.to_html(full_html=False, include_plotlyjs='cdn')

    # Render the `dashboard.html` from the "templates" folder and inject the Plotly chart
    return render_template('templates/dashboard.html', chart_html=chart_html)


if __name__ == "__main__":
    app.run(debug=True)
