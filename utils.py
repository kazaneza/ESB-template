# utils.py

from data.datahub import cache
import threading

services = [
    'TELCO_PUSH_TRANS',
    'TELCO_PULL_TRANS',
    'TAX_PAYMENT_TRNX_MTN',
    'TELCO_IREMBO_TRANS',
    'AIRTEL_IREMBO_TRANS',
    'EKash_transfer'
]

def get_success_rates():
    success_rates = {}
    for service in services:
        service_cache = cache.get(service, {})
        success_rates[service] = {
            'success_rate': service_cache.get('success_rate', "N/A"),
            'moving_average_amount': service_cache.get('moving_average_amount', "N/A")
        }
    return success_rates

def start_datahub_thread():
    from data.datahub import start_cache_updater  # Avoid circular imports

    def run_datahub():
        start_cache_updater()

    datahub_thread = threading.Thread(target=run_datahub)
    datahub_thread.daemon = True
    datahub_thread.start()
