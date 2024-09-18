from flask import Flask, render_template
from graph.graph_generator import create_graph

app = Flask(__name__, template_folder='templates')

@app.route('/')
def index():
    graph_url = create_graph()
    return render_template('dashboard.html', graph_url=graph_url)

if __name__ == '__main__':
    app.run(debug=True)
