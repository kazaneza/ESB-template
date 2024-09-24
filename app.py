from flask import Flask, render_template, jsonify
from data.datahub import start_cache_updater, cache
import threading

app = Flask(__name__)

# Start the cache updater in a separate thread
def run_datahub():
    start_cache_updater()

datahub_thread = threading.Thread(target=run_datahub)
datahub_thread.daemon = True
datahub_thread.start()

@app.route('/')
def index():
    # Fetch success rates for each service from the cache
    success_rates = {
        'TELCO_PUSH_TRANS': cache.get('TELCO_PUSH_TRANS_success_rate', {}).get('success_rate', "N/A"),
        'TELCO_PULL_TRANS': cache.get('TELCO_PULL_TRANS_success_rate', {}).get('success_rate', "N/A"),
        'TAX_PAYMENT_TRNX_MTN': cache.get('TAX_PAYMENT_TRNX_MTN_success_rate', {}).get('success_rate', "N/A"),
        'TELCO_IREMBO_TRANS': cache.get('TELCO_IREMBO_TRANS_success_rate', {}).get('success_rate', "N/A"),
        'AIRTEL_IREMBO_TRANS': cache.get('AIRTEL_IREMBO_TRANS_success_rate', {}).get('success_rate', "N/A"),
        'EKash_transfer': cache.get('EKash_transfer_success_rate', {}).get('success_rate', "N/A"),
    }

    return render_template('dashboard.html', success_rates=success_rates)

@app.route('/api/success_rates', methods=['GET'])
def get_success_rates():
    success_rates = {
        'TELCO_PUSH_TRANS': cache.get('TELCO_PUSH_TRANS_success_rate', {}).get('success_rate', "N/A"),
        'TELCO_PULL_TRANS': cache.get('TELCO_PULL_TRANS_success_rate', {}).get('success_rate', "N/A"),
        'TAX_PAYMENT_TRNX_MTN': cache.get('TAX_PAYMENT_TRNX_MTN_success_rate', {}).get('success_rate', "N/A"),
        'TELCO_IREMBO_TRANS': cache.get('TELCO_IREMBO_TRANS_success_rate', {}).get('success_rate', "N/A"),
        'AIRTEL_IREMBO_TRANS': cache.get('AIRTEL_IREMBO_TRANS_success_rate', {}).get('success_rate', "N/A"),
        'EKash_transfer': cache.get('EKash_transfer_success_rate', {}).get('success_rate', "N/A"),
    }

    return jsonify(success_rates)

if __name__ == '__main__':
    app.run(debug=True)
