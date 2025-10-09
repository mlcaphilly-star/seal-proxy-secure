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

// Root route
app.get('/', (req, res) => {
  res.send('Seal Proxy Secure Server is running! ✅');
});

// SQLite setup
const db = new sqlite3.Database(path.join(__dirname, 'vacations.db'), (err) => {
  if (err) {
    console.error('Failed to open SQLite DB:', err);
    process.exit(1);
  }
  console.log('Connected to SQLite database.');
});

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

// ✅ 1. Fetch enrollments for logged-in customer (by email)
app.get('/enrollments', async (req, res) => {
  const email = req.query.email;
  if (!email) return res.status(400).json({ error: "Missing 'email' parameter" });

  try {
    const sealRes = await fetch(`https://app.sealsubscriptions.com/shopify/merchant/api/subscriptions?query=${encodeURIComponent(email)}`, {
      headers: { 'X-Seal-Token': SEAL_TOKEN, 'Content-Type': 'application/json' },
    });
    const sealData = await sealRes.json();

    if (!sealRes.ok) {
      return res.status(sealRes.status).json({ error: sealData.error || "Failed to fetch subscriptions" });
    }

    const enrollments = (sealData.payload.subscriptions || []).map(sub => {
      const item = sub.items[0];
      const props = item.properties || [];
      const get = key => (props.find(p => p.key === key)?.value || '');

      return {
        subscription_id: sub.id,
        child_first_name: get('Child First Name'),
        child_last_name: get('Child Last Name'),
        cricclub_id: get('Child CricClub ID'),
        program: get('Program Level').replace('coach-', ''),
        payment_frequency: get('Billing Interval'),
        next_payment_date: sub.billing_attempts?.[0]?.date || '',
        next_payment_amount: item.final_price || '',
        parent_email: email
      };
    });

    res.json({ success: true, enrollments });
  } catch (err) {
    console.error('Error fetching enrollments:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// ✅ 2. Fetch vacation details for a given child
app.get('/vacations', (req, res) => {
  const { email, child_name } = req.query;
  if (!email || !child_name) {
    return res.status(400).json({ error: "Missing 'email' or 'child_name'" });
  }

  const sql = `
    SELECT child_name, from_date, to_date, reason, shift_days
    FROM vacation_requests
    WHERE customer_id = ? AND child_name = ?
    ORDER BY from_date DESC
  `;
  db.all(sql, [email, child_name], (err, rows) => {
    if (err) return res.status(500).json({ error: 'Database error' });
    res.json({ success: true, vacations: rows });
  });
});

// ✅ 3. Fetch billing schedule for a given subscription ID
app.get('/billing-schedule', async (req, res) => {
  const subscriptionId = req.query.subscription_id;
  if (!subscriptionId) return res.status(400).json({ error: "Missing 'subscription_id'" });

  try {
    const sealRes = await fetch(`https://app.sealsubscriptions.com/shopify/merchant/api/subscription?id=${subscriptionId}`, {
      headers: { 'X-Seal-Token': SEAL_TOKEN, 'Content-Type': 'application/json' },
    });
    const data = await sealRes.json();

    if (!sealRes.ok) return res.status(sealRes.status).json({ error: data.error || "Failed to fetch billing schedule" });

    const billing_attempts = (data.payload.billing_attempts || []).slice(0, 4).map(a => ({
      date: a.date,
      amount: data.payload.items?.[0]?.final_price || '',
    }));

    res.json({ success: true, billing_attempts });
  } catch (err) {
    console.error('Error fetching billing schedule:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

app.listen(PORT, () => console.log(`Proxy server listening on http://localhost:${PORT}`));
