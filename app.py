from flask import Flask, render_template, jsonify
from utils import get_success_rates, start_datahub_thread
from graphs import create_all_graphs
from data.overall_datahub import overall_cache, start_overall_metrics_updater
from data.error_datahub import get_top_5_errors
from data.current_datahub import get_cached_transactions 

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
    overall_metrics = overall_cache
    top_errors = get_top_5_errors()
    latest_transactions = get_cached_transactions()

    return render_template(
        'dashboard.html',
        success_rates=success_rates,
        overall_metrics=overall_metrics,
        top_errors=top_errors,
        latest_transactions=latest_transactions,
        **graphs
    )
@app.route('/api/latest_transactions', methods=['GET'])
def api_get_latest_transactions():
    return jsonify(get_cached_transactions())

@app.route('/api/top_errors', methods=['GET'])
def api_get_top_errors():
    top_errors = get_top_5_errors()
    return jsonify(top_errors)


@app.route('/api/success_rates', methods=['GET'])
def api_get_success_rates():
    success_rates = get_success_rates(services)
    return jsonify(success_rates)

@app.route('/api/overall_metrics', methods=['GET'])
def api_get_overall_metrics():
    return jsonify(overall_cache)

if __name__ == '__main__':
    app.run(debug=True)
