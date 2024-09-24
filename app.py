from flask import Flask, render_template, jsonify
from data.datahub import start_cache_updater, cache
from graph.graph1 import create_graph_failure
from graph.graph2 import create_graph_success
from graph.graph3 import create_graph_pending

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
    
    graph_failure = create_graph_failure()  
    graph_success = create_graph_success()  
    graph_pending = create_graph_pending()  

    # Fetch success rates and moving averages for each service from the cache
    success_rates = {
        'TELCO_PUSH_TRANS': {
            'success_rate': cache.get('TELCO_PUSH_TRANS', {}).get('success_rate', "N/A"),
            'moving_average_amount': cache.get('TELCO_PUSH_TRANS', {}).get('moving_average_amount', "N/A")
        },
        'TELCO_PULL_TRANS': {
            'success_rate': cache.get('TELCO_PULL_TRANS', {}).get('success_rate', "N/A"),
            'moving_average_amount': cache.get('TELCO_PULL_TRANS', {}).get('moving_average_amount', "N/A")
        },
        'TAX_PAYMENT_TRNX_MTN': {
            'success_rate': cache.get('TAX_PAYMENT_TRNX_MTN', {}).get('success_rate', "N/A"),
            'moving_average_amount': cache.get('TAX_PAYMENT_TRNX_MTN', {}).get('moving_average_amount', "N/A")
        },
        'TELCO_IREMBO_TRANS': {
            'success_rate': cache.get('TELCO_IREMBO_TRANS', {}).get('success_rate', "N/A"),
            'moving_average_amount': cache.get('TELCO_IREMBO_TRANS', {}).get('moving_average_amount', "N/A")
        },
        'AIRTEL_IREMBO_TRANS': {
            'success_rate': cache.get('AIRTEL_IREMBO_TRANS', {}).get('success_rate', "N/A"),
            'moving_average_amount': cache.get('AIRTEL_IREMBO_TRANS', {}).get('moving_average_amount', "N/A")
        },
        'EKash_transfer': {
            'success_rate': cache.get('EKash_transfer', {}).get('success_rate', "N/A"),
            'moving_average_amount': cache.get('EKash_transfer', {}).get('moving_average_amount', "N/A")
        }
    }

    return render_template('dashboard.html', success_rates=success_rates,
                           create_graph_failure=graph_failure,
                           create_graph_success=graph_success,
                           create_graph_pending=graph_pending)

@app.route('/api/success_rates', methods=['GET'])
def get_success_rates():
    success_rates = {
        'TELCO_PUSH_TRANS': {
            'success_rate': cache.get('TELCO_PUSH_TRANS', {}).get('success_rate', "N/A"),
            'moving_average_amount': cache.get('TELCO_PUSH_TRANS', {}).get('moving_average_amount', "N/A")
        },
        'TELCO_PULL_TRANS': {
            'success_rate': cache.get('TELCO_PULL_TRANS', {}).get('success_rate', "N/A"),
            'moving_average_amount': cache.get('TELCO_PULL_TRANS', {}).get('moving_average_amount', "N/A")
        },
        'TAX_PAYMENT_TRNX_MTN': {
            'success_rate': cache.get('TAX_PAYMENT_TRNX_MTN', {}).get('success_rate', "N/A"),
            'moving_average_amount': cache.get('TAX_PAYMENT_TRNX_MTN', {}).get('moving_average_amount', "N/A")
        },
        'TELCO_IREMBO_TRANS': {
            'success_rate': cache.get('TELCO_IREMBO_TRANS', {}).get('success_rate', "N/A"),
            'moving_average_amount': cache.get('TELCO_IREMBO_TRANS', {}).get('moving_average_amount', "N/A")
        },
        'AIRTEL_IREMBO_TRANS': {
            'success_rate': cache.get('AIRTEL_IREMBO_TRANS', {}).get('success_rate', "N/A"),
            'moving_average_amount': cache.get('AIRTEL_IREMBO_TRANS', {}).get('moving_average_amount', "N/A")
        },
        'EKash_transfer': {
            'success_rate': cache.get('EKash_transfer', {}).get('success_rate', "N/A"),
            'moving_average_amount': cache.get('EKash_transfer', {}).get('moving_average_amount', "N/A")
        }
    }

    return jsonify(success_rates)

if __name__ == '__main__':
    app.run(debug=True)
