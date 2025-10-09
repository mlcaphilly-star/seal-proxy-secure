require('dotenv').config();
const express = require('express');
const fetch = require('node-fetch');
const cors = require('cors');
const helmet = require('helmet');

const app = express();
const PORT = process.env.PORT || 3000;
const SEAL_TOKEN = process.env.SEAL_TOKEN;
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN;

if (!SEAL_TOKEN || !ALLOWED_ORIGIN) {
  console.error('ERROR: SEAL_TOKEN and ALLOWED_ORIGIN must be set in .env file');
  process.exit(1);
}

app.use(helmet());
app.use(cors({ origin: ALLOWED_ORIGIN }));
app.use(express.json());

// Endpoint to get all subscriptions for a customer
app.get('/seal-subscriptions', async (req, res) => {
  const customerId = req.query.customer_id;
  if (!customerId || !/^\d+$/.test(customerId)) {
    return res.status(400).json({ error: "Invalid or missing 'customer_id' query parameter" });
  }

  try {
    const apiRes = await fetch(`https://app.sealsubscriptions.com/shopify/merchant/api/subscriptions?customer_id=${customerId}`, {
      headers: {
        'X-Seal-Token': SEAL_TOKEN,
        'Content-Type': 'application/json',
      }
    });

    if (!apiRes.ok) {
      const errorText = await apiRes.text();
      return res.status(apiRes.status).json({ error: `Seal API error: ${apiRes.status} - ${errorText}` });
    }

    const data = await apiRes.json();
    res.json(data);

  } catch (err) {
    console.error('Proxy server internal error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Endpoint to get detailed subscription info by subscription id
app.get('/seal-subscription', async (req, res) => {
  const subscriptionId = req.query.id;
  if (!subscriptionId || !/^\d+$/.test(subscriptionId)) {
    return res.status(400).json({ error: "Invalid or missing 'id' query parameter" });
  }

  try {
    const apiRes = await fetch(`https://app.sealsubscriptions.com/shopify/merchant/api/subscription?id=${subscriptionId}`, {
      headers: {
        'X-Seal-Token': SEAL_TOKEN,
        'Content-Type': 'application/json',
      }
    });

    if (!apiRes.ok) {
      const errorText = await apiRes.text();
      return res.status(apiRes.status).json({ error: `Seal API error: ${apiRes.status} - ${errorText}` });
    }

    const data = await apiRes.json();
    res.json(data);

  } catch (err) {
    console.error('Proxy server internal error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Reschedule a billing attempt
app.put('/reschedule-billing-attempt', async (req, res) => {
  const { billing_attempt_id, subscription_id, date, time, timezone } = req.body;

  if (!billing_attempt_id || !subscription_id || !date || !time || !timezone) {
    return res.status(400).json({ error: "Missing required fields" });
  }

  try {
    const sealRes = await fetch('https://app.sealsubscriptions.com/shopify/merchant/api/subscription-billing-attempt', {
      method: 'PUT',
      headers: {
        'X-Seal-Token': SEAL_TOKEN,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        id: billing_attempt_id,
        subscription_id,
        date,
        time,
        timezone,
        action: "reschedule",
        reset_schedule: true
      })
    });

    const result = await sealRes.json();
    if (!sealRes.ok) {
      return res.status(sealRes.status).json({ error: result.error || "Failed to reschedule" });
    }

    res.json({ success: true, result });
  } catch (err) {
    console.error('Error rescheduling billing:', err);
    res.status(500).json({ error: "Internal server error" });
  }
});

// POST vacation request (mock implementation)
app.post('/vacation-request', async (req, res) => {
  const {
    customer_id,
    child_name,
    from_date,
    to_date,
    shift_days,
    reason,
    subscription_id,
    billing_attempt_id,
  } = req.body;

  if (
    !customer_id ||
    !child_name ||
    !from_date ||
    !to_date ||
    !shift_days ||
    !subscription_id ||
    !billing_attempt_id
  ) {
    return res.status(400).json({ success: false, error: 'Missing required fields' });
  }

  try {
    // TODO: Add your logic here to save the vacation request in your DB or other system

    // Mock response with updated billing attempts:
    const updatedBillingAttempts = [
      { id: 1, date: '2025-09-01T00:00:00Z' },
      { id: 2, date: '2025-10-01T00:00:00Z' },
      { id: 3, date: '2025-11-01T00:00:00Z' },
      { id: 4, date: '2025-12-01T00:00:00Z' },
      { id: 5, date: '2026-01-01T00:00:00Z' },
    ];

    res.json({ success: true, updated: { billing_attempts: updatedBillingAttempts } });
  } catch (err) {
    console.error('Error processing vacation request:', err);
    res.status(500).json({ success: false, error: 'Internal server error' });
  }
});

app.listen(PORT, () => {
  console.log(`Proxy server listening on http://localhost:${PORT}`);
});
