from flask import Flask, render_template, jsonify
from utils import get_success_rates, start_datahub_thread
from graphs import create_all_graphs  

app = Flask(__name__)

# Start the cache updater thread
start_datahub_thread()

@app.route('/')
def index():
    graphs = create_all_graphs()
    success_rates = get_success_rates()

    return render_template(
        'dashboard.html',
        success_rates=success_rates,
        **graphs
    )

@app.route('/api/success_rates', methods=['GET'])
def api_get_success_rates():
    success_rates = get_success_rates()
    return jsonify(success_rates)

if __name__ == '__main__':
    app.run(debug=True)
