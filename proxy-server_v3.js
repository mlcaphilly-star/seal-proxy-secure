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

// Initialize SQLite DB, stored locally as 'vacations.db'
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

// Endpoint to get all subscriptions for a customer (same as before)
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

// Endpoint to get detailed subscription info by subscription id (same as before)
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

// Reschedule a billing attempt (same as before)
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

// POST vacation request: save to SQLite and check for overlapping vacations
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

  // Check overlapping vacation for same customer + child
  // Overlap condition: new.from_date <= existing.to_date AND new.to_date >= existing.from_date
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
    if (err) {
      console.error('SQLite error on overlap check:', err);
      return res.status(500).json({ success: false, error: 'Database error' });
    }

    if (row) {
      // Overlap found - send user-friendly message
      return res.status(409).json({
        success: false,
        error: `You have already submitted a vacation request from ${row.from_date} to ${row.to_date}. Overlapping requests are not allowed.`
      });
    }

    // No overlap, insert new vacation request
    const sqlInsert = `
      INSERT INTO vacation_requests (
        customer_id, child_name, from_date, to_date, shift_days, reason, subscription_id, billing_attempt_id
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `;

    db.run(sqlInsert, [customer_id, child_name, from_date, to_date, shift_days, reason, subscription_id, billing_attempt_id], function(insertErr) {
      if (insertErr) {
        console.error('SQLite insert error:', insertErr);
        return res.status(500).json({ success: false, error: 'Failed to save vacation request' });
      }

      // Return mock updated billing attempts, replace with your logic if needed
      const updatedBillingAttempts = [
        { id: 1, date: '2025-09-01T00:00:00Z' },
        { id: 2, date: '2025-10-01T00:00:00Z' },
        { id: 3, date: '2025-11-01T00:00:00Z' },
        { id: 4, date: '2025-12-01T00:00:00Z' },
        { id: 5, date: '2026-01-01T00:00:00Z' },
      ];

      return res.json({ success: true, updated: { billing_attempts: updatedBillingAttempts }, id: this.lastID });
    });
  });
});

app.listen(PORT, () => {
  console.log(`Proxy server listening on http://localhost:${PORT}`);
});
