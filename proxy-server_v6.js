require('dotenv').config();
const express = require('express');
const fetch = require('node-fetch');
const cors = require('cors');
const helmet = require('helmet');
const sqlite3 = require('sqlite3').verbose();
const path = require('path');

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

// Root check route
app.get('/', (req, res) => {
  res.send('Seal Proxy Secure Server is running! ✅');
});

// Initialize SQLite DB
const db = new sqlite3.Database(path.join(__dirname, 'vacations.db'), (err) => {
  if (err) {
    console.error('Failed to open SQLite DB:', err);
    process.exit(1);
  }
  console.log('Connected to SQLite database.');
});

// Create vacation_requests table if not exists
db.run(`
  CREATE TABLE IF NOT EXISTS vacation_requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id TEXT NOT NULL,
    child_name TEXT NOT NULL,
    from_date TEXT NOT NULL,
    to_date TEXT NOT NULL,
    shift_days INTEGER NOT NULL,
    reason TEXT,
    subscription_id TEXT NOT NULL,
    billing_attempt_id TEXT NOT NULL
  )
`);

// Updated /seal-subscriptions endpoint using email-based query
app.get('/seal-subscriptions', async (req, res) => {
  const email = req.query.email;
  if (!email) {
    return res.status(400).json({ error: "Missing 'email' query parameter" });
  }

  try {
    const apiRes = await fetch(`https://app.sealsubscriptions.com/shopify/merchant/api/subscriptions?query=${encodeURIComponent(email)}`, {
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
    res.json({ success: true, payload: { subscriptions: data.payload.subscriptions || [] } });

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

// POST vacation request: save to SQLite and check for overlapping vacations + start date validation
app.post('/vacation-request', (req, res) => {
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

  // Fetch subscription details to validate first billing attempt
  fetch(`https://app.sealsubscriptions.com/shopify/merchant/api/subscription?id=${subscription_id}`, {
    headers: { 'X-Seal-Token': SEAL_TOKEN, 'Content-Type': 'application/json' }
  })
    .then(async sealRes => {
      if (!sealRes.ok) {
        const errText = await sealRes.text();
        console.error('Seal API fetch subscription failed:', errText);
        return res.status(502).json({ success: false, error: 'Failed to fetch subscription details from Seal' });
      }
      return sealRes.json();
    })
    .then(subscriptionData => {
      const billingAttempts = subscriptionData.payload.billing_attempts || [];
      if (billingAttempts.length === 0) return null;

      const firstBillingAttemptDate = new Date(billingAttempts[0].date);
      if (new Date(from_date) > firstBillingAttemptDate) {
        return res.status(400).json({
          success: false,
          error: `You Cannot submit this request now. Please submit it after ${firstBillingAttemptDate.toISOString().slice(0,10)}`
        });
      }
      return null;
    })
    .then(() => {
      const sqlCheck = `
        SELECT * FROM vacation_requests
        WHERE customer_id = ?
          AND child_name = ?
          AND (
            (date(?) <= date(to_date) AND date(?) >= date(from_date))
            OR
            (date(?) <= date(to_date) AND date(?) >= date(from_date))
            OR
            (date(from_date) <= date(?) AND date(to_date) >= date(?))
          )
        LIMIT 1
      `;
      db.get(sqlCheck, [customer_id, child_name, from_date, from_date, to_date, to_date, from_date, to_date], (err, row) => {
        if (err) return res.status(500).json({ success: false, error: 'Database error' });
        if (row) return res.status(409).json({ success: false, error: `You have already submitted a vacation request from ${row.from_date} to ${row.to_date}. Overlapping requests are not allowed.` });

        const sqlInsert = `
          INSERT INTO vacation_requests (
            customer_id, child_name, from_date, to_date, shift_days, reason, subscription_id, billing_attempt_id
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        `;

        db.run(sqlInsert, [customer_id, child_name, from_date, to_date, shift_days, reason, subscription_id, billing_attempt_id], async function(insertErr) {
          if (insertErr) return res.status(500).json({ success: false, error: 'Failed to save vacation request' });

          try {
            const sealRes = await fetch(`https://app.sealsubscriptions.com/shopify/merchant/api/subscription?id=${subscription_id}`, {
              headers: { 'X-Seal-Token': SEAL_TOKEN, 'Content-Type': 'application/json' }
            });

            if (!sealRes.ok) {
              const errText = await sealRes.text();
              return res.status(502).json({ success: false, error: 'Failed to fetch subscription details from Seal' });
            }

            const subscriptionData = await sealRes.json();
            const billingAttempts = subscriptionData.payload.billing_attempts || [];
            const updatedBillingAttempts = billingAttempts.map(attempt => {
              const origDate = new Date(attempt.date);
              origDate.setDate(origDate.getDate() + shift_days);
              return { ...attempt, original_date: attempt.date, date: origDate.toISOString() };
            });

            return res.json({ success: true, updated: { billing_attempts: updatedBillingAttempts }, id: this.lastID });

          } catch (fetchErr) {
            return res.status(500).json({ success: false, error: 'Error processing subscription billing attempts' });
          }
        });
      });
    })
    .catch(err => {
      if (!res.headersSent) res.status(500).json({ success: false, error: 'Internal server error' });
    });
});

app.listen(PORT, () => {
  console.log(`Proxy server listening on http://localhost:${PORT}`);
});
