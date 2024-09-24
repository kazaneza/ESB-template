async function fetchSuccessRates() {
    try {
        const response = await fetch('/api/success_rates');
        const data = await response.json();

        const services = [
            'TELCO_PUSH_TRANS', 
            'TELCO_PULL_TRANS', 
            'TAX_PAYMENT_TRNX_MTN', 
            'TELCO_IREMBO_TRANS', 
            'AIRTEL_IREMBO_TRANS', 
            'EKash_transfer'
        ];

        services.forEach(service => {
            const successRateElement = document.getElementById(`${service}-success-rate`);
            const movingAverageElement = document.getElementById(`${service}-moving-average`);
            
            if (successRateElement) {
                successRateElement.innerText = data[service].success_rate + '%';
            }

            if (movingAverageElement) {
                movingAverageElement.innerText = data[service].moving_average_amount;
            }
        });
    } catch (error) {
        console.error('Error fetching success rates:', error);
    }
}

window.onload = function() {
    fetchSuccessRates();
    setInterval(fetchSuccessRates, 10000); // Refresh every 10 seconds
};
