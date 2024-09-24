async function fetchSuccessRates() {
    console.log('Script is running');  // Check if the script is running
    try {
        // Fetch data from the Flask API
        const response = await fetch('/api/success_rates');
        const data = await response.json();  // Parse the JSON response

        console.log('Fetched success rates:', data);  // Log the fetched data

        // Update success rates for each service
        document.getElementById('TELCO_PUSH_TRANS-success-rate').innerText = data.TELCO_PUSH_TRANS + '%';
        document.getElementById('TELCO_PULL_TRANS-success-rate').innerText = data.TELCO_PULL_TRANS + '%';
        document.getElementById('TAX_PAYMENT_TRNX_MTN-success-rate').innerText = data.TAX_PAYMENT_TRNX_MTN + '%';
        document.getElementById('TELCO_IREMBO_TRANS-success-rate').innerText = data.TELCO_IREMBO_TRANS + '%';
        document.getElementById('EKash_transfer-success-rate').innerText = data.EKash_transfer + '%';
        document.getElementById('AIRTEL_IREMBO_TRANS-success-rate').innerText = data.AIRTEL_IREMBO_TRANS + '%';
    } catch (error) {
        console.error('Error fetching success rates:', error);
    }
}

// Call the function when the page loads
window.onload = function() {
    fetchSuccessRates();
    setInterval(fetchSuccessRates, 10000);  // Refresh the rates every 10 seconds
};
