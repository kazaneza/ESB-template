// Function to fetch and update Success Rates
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

// Function to fetch and update Overall Metrics
async function fetchOverallMetrics() {
    try {
        const response = await fetch('/api/overall_metrics');
        const data = await response.json();

        const metricsMap = {
            'Overall_Total_Transactions': 'overall-total-transactions',
            'Overall_Total_Amount': 'overall-total-amount',
            'Overall_Successful_Transactions': 'overall-successful-transactions',
            'Overall_Successful_Amount': 'overall-successful-amount',
            'Overall_Success_Rate': 'overall-success-rate',
            'Overall_Pending_Transactions': 'overall-pending-transactions',
            'Overall_Pending_Amount': 'overall-pending-amount',
            'Overall_Pending_Rate': 'overall-pending-rate',
            'Overall_Failed_Transactions': 'overall-failed-transactions',
            'Overall_Failed_Amount': 'overall-failed-amount',
            'Overall_Failure_Rate': 'overall-failure-rate'
        };

        for (const [key, elementId] of Object.entries(metricsMap)) {
            const element = document.getElementById(elementId);
            if (element) {
                let value = data[key];

                // Format numbers with commas and two decimal places
                if (typeof value === 'number') {
                    value = value.toLocaleString(undefined, { 
                        minimumFractionDigits: 2, 
                        maximumFractionDigits: 2 
                    });
                }

                // Append '%' for rate metrics
                if (key.includes('Rate')) {
                    value = value + '%';
                }

                element.innerText = value;
            }
        }
    } catch (error) {
        console.error('Error fetching overall metrics:', error);
    }
}

// Unified function to update the dashboard
async function updateDashboard() {
    await Promise.all([fetchSuccessRates(), fetchOverallMetrics()]);
}

// Initialize the dashboard on window load and set intervals
window.onload = function() {
    updateDashboard(); // Initial fetch on page load
    setInterval(updateDashboard, 10000); // Refresh every 10 seconds
};
