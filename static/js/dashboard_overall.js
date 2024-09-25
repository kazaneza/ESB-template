async function fetchOverallMetrics() {
    try {
        const response = await fetch('/api/overall_metrics');
        const data = await response.json();

        // Map of metric keys to HTML element IDs
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

        // Update the HTML elements with the data
        for (const [key, elementId] of Object.entries(metricsMap)) {
            const element = document.getElementById(elementId);
            if (element) {
                let value = data[key];

                // Format numbers with commas and two decimal places
                if (typeof value === 'number') {
                    value = value.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
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

window.onload = function() {
    fetchOverallMetrics();
    setInterval(fetchOverallMetrics, 10000); // Refresh every 10 seconds
};
