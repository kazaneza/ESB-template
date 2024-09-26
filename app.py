from flask import Flask, render_template, jsonify
from utils import get_success_rates, start_datahub_thread
from graphs import create_all_graphs

# Import the new module for overall metrics
from data.overall_datahub import overall_cache, start_overall_metrics_updater

app = Flask(__name__)

# Start the cache updater threads
start_datahub_thread()
start_overall_metrics_updater()

services = [
    'TELCO_PUSH_TRANS',
    'TELCO_PULL_TRANS',
    'TAX_PAYMENT_TRNX_MTN',
    'TELCO_IREMBO_TRANS',
    'AIRTEL_IREMBO_TRANS',
    'EKash_transfer'
]

@app.route('/')
def index():
    graphs = create_all_graphs()
    success_rates = get_success_rates(services)
    overall_metrics = overall_cache  # Get the overall metrics

    return render_template(
        'dashboard.html',
        success_rates=success_rates,
        overall_metrics=overall_metrics,
        **graphs
    )

@app.route('/api/success_rates', methods=['GET'])
def api_get_success_rates():
    success_rates = get_success_rates(services)
    return jsonify(success_rates)

@app.route('/api/overall_metrics', methods=['GET'])
def api_get_overall_metrics():
    return jsonify(overall_cache)

if __name__ == '__main__':
    app.run(debug=True)
