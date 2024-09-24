async function fetchSuccessRates() {
    try {
        // Fetch data from the Flask API
        const response = await fetch('/api/success_rates');
        const data = await response.json();  // Parse the JSON response

        console.log('Fetched success rates:', data);  // Log the fetched data

        // Update success rates and moving averages for each service
        const pushSuccessRate = document.getElementById('TELCO_PUSH_TRANS-success-rate');
        if (pushSuccessRate) {
            pushSuccessRate.innerText = data.TELCO_PUSH_TRANS.success_rate + '%';
        } else {
            console.error("Element 'TELCO_PUSH_TRANS-success-rate' not found");
        }

        const pushMovingAvg = document.getElementById('TELCO_PUSH_TRANS-moving-average');
        if (pushMovingAvg) {
            pushMovingAvg.innerText = data.TELCO_PUSH_TRANS.moving_average_amount;
        } else {
            console.error("Element 'TELCO_PUSH_TRANS-moving-average' not found");
        }

        const pullSuccessRate = document.getElementById('TELCO_PULL_TRANS-success-rate');
        if (pullSuccessRate) {
            pullSuccessRate.innerText = data.TELCO_PULL_TRANS.success_rate + '%';
        } else {
            console.error("Element 'TELCO_PULL_TRANS-success-rate' not found");
        }

        const pullMovingAvg = document.getElementById('TELCO_PULL_TRANS-moving-average');
        if (pullMovingAvg) {
            pullMovingAvg.innerText = data.TELCO_PULL_TRANS.moving_average_amount;
        } else {
            console.error("Element 'TELCO_PULL_TRANS-moving-average' not found");
        }

        const taxSuccessRate = document.getElementById('TAX_PAYMENT_TRNX_MTN-success-rate');
        if (taxSuccessRate) {
            taxSuccessRate.innerText = data.TAX_PAYMENT_TRNX_MTN.success_rate + '%';
        } else {
            console.error("Element 'TAX_PAYMENT_TRNX_MTN-success-rate' not found");
        }

        const taxMovingAvg = document.getElementById('TAX_PAYMENT_TRNX_MTN-moving-average');
        if (taxMovingAvg) {
            taxMovingAvg.innerText = data.TAX_PAYMENT_TRNX_MTN.moving_average_amount;
        } else {
            console.error("Element 'TAX_PAYMENT_TRNX_MTN-moving-average' not found");
        }

        const iremboSuccessRate = document.getElementById('TELCO_IREMBO_TRANS-success-rate');
        if (iremboSuccessRate) {
            iremboSuccessRate.innerText = data.TELCO_IREMBO_TRANS.success_rate + '%';
        } else {
            console.error("Element 'TELCO_IREMBO_TRANS-success-rate' not found");
        }

        const iremboMovingAvg = document.getElementById('TELCO_IREMBO_TRANS-moving-average');
        if (iremboMovingAvg) {
            iremboMovingAvg.innerText = data.TELCO_IREMBO_TRANS.moving_average_amount;
        } else {
            console.error("Element 'TELCO_IREMBO_TRANS-moving-average' not found");
        }

        const airtelSuccessRate = document.getElementById('AIRTEL_IREMBO_TRANS-success-rate');
        if (airtelSuccessRate) {
            airtelSuccessRate.innerText = data.AIRTEL_IREMBO_TRANS.success_rate + '%';
        } else {
            console.error("Element 'AIRTEL_IREMBO_TRANS-success-rate' not found");
        }

        const airtelMovingAvg = document.getElementById('AIRTEL_IREMBO_TRANS-moving-average');
        if (airtelMovingAvg) {
            airtelMovingAvg.innerText = data.AIRTEL_IREMBO_TRANS.moving_average_amount;
        } else {
            console.error("Element 'AIRTEL_IREMBO_TRANS-moving-average' not found");
        }

        const ekashSuccessRate = document.getElementById('EKash_transfer-success-rate');
        if (ekashSuccessRate) {
            ekashSuccessRate.innerText = data.EKash_transfer.success_rate + '%';
        } else {
            console.error("Element 'EKash_transfer-success-rate' not found");
        }

        const ekashMovingAvg = document.getElementById('EKash_transfer-moving-average');
        if (ekashMovingAvg) {
            ekashMovingAvg.innerText = data.EKash_transfer.moving_average_amount;
        } else {
            console.error("Element 'EKash_transfer-moving-average' not found");
        }

    } catch (error) {
        console.error('Error updating the elements:', error);
    }
}

// Call the function when the page loads
window.onload = function() {
    console.log("Running success rates script");
    fetchSuccessRates();  // Fetch data and update the HTML
    setInterval(fetchSuccessRates, 10000);  // Refresh the rates every 10 seconds
};
