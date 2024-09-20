from flask import Flask, render_template
from graph.graph1 import create_graph_failure
from graph.graph2 import create_graph_success
from graph.graph3 import create_graph_pending

app = Flask(__name__)

@app.route('/')
def index():
    graph_failure_url = create_graph_failure() 
    graph_success_url = create_graph_success() 
    graph_pending_url = create_graph_pending() 
    
    time_ranges = ['Today', 'Last 7 Days', 'Last 30 Days', 'This Month', 'Last Month', 'Custom Range']
    return render_template('dashboard.html', 
                           create_graph_pending=graph_pending_url, 
                           create_graph_success=graph_success_url, 
                           create_graph_failure=graph_failure_url,
                           time_ranges=time_ranges)
if __name__ == '__main__':
    app.run(debug=True)
