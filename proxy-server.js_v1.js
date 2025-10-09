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

app.listen(PORT, () => {
  console.log(`Proxy server listening on http://localhost:${PORT}`);
});
