# api_routes.py
from flask import Blueprint, jsonify
from data.datahub import cache  # Import your cache or data access layer

api_routes = Blueprint('api_routes', __name__)

@api_routes.route('/api/success_rates', methods=['GET'])
def get_success_rates():
    # Example of fetching success rates for 6 different services
    service1_data = cache.get('service1_success_rate')
    service2_data = cache.get('service2_success_rate')
    service3_data = cache.get('service3_success_rate')
    service4_data = cache.get('service4_success_rate')
    service5_data = cache.get('service5_success_rate')
    service6_data = cache.get('service6_success_rate')

    # Handling the case where the data might not be available
    success_rates = {
        'service1': service1_data['success_rate'] if service1_data else "N/A",
        'service2': service2_data['success_rate'] if service2_data else "N/A",
        'service3': service3_data['success_rate'] if service3_data else "N/A",
        'service4': service4_data['success_rate'] if service4_data else "N/A",
        'service5': service5_data['success_rate'] if service5_data else "N/A",
        'service6': service6_data['success_rate'] if service6_data else "N/A",
    }

    return jsonify(success_rates)
