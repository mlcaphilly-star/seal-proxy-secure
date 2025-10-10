require('dotenv').config();
const express = require('express');
const fetch = require('node-fetch');
const cors = require('cors');
const helmet = require('helmet');
const { Pool } = require('pg');

const app = express();
const PORT = process.env.PORT || 3000;
const SEAL_TOKEN = process.env.SEAL_TOKEN;
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN;

// Postgres pool
// Use your Render Postgres URL
const pool = new Pool({
  connectionString: 'postgresql://apps:pI9skLOqhlZsVTCpUae0E4YnvgzhG7o8@dpg-d3k8q9l6ubrc73dp5afg-a.oregon-postgres.render.com/vacation_db_gdge',
  ssl: {
    rejectUnauthorized: false  // Required for Render Postgres
  }
});
// Test connection
pool.connect()
  .then(() => console.log('Connected to Postgres'))
  .catch(err => console.error('Postgres connection error', err));


app.use(helmet());
app.use(cors({ origin: ALLOWED_ORIGIN }));
app.use(express.json());

// Initialize DB table
pool.query(`
CREATE TABLE IF NOT EXISTS vacation_requests (
  id SERIAL PRIMARY KEY,
  customer_id TEXT NOT NULL,
  child_name TEXT NOT NULL,
  from_date DATE NOT NULL,
  to_date DATE NOT NULL,
  shift_days INT NOT NULL,
  reason TEXT,
  subscription_id TEXT NOT NULL,
  billing_attempt_id TEXT NOT NULL
)
`).catch(err => console.error('Error creating table:', err));

app.get('/', (req, res) => {
  res.send('Seal Proxy Secure Server is running! ✅');
});
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
// Enrollments endpoint (same as before)
app.get("/enrollments", async (req, res) => {
  const email = req.query.email;
  if (!email) return res.status(400).json({ success: false, error: "Missing email" });

  try {
    const subsResponse = await fetch(
      `https://app.sealsubscriptions.com/shopify/merchant/api/subscriptions?query=${encodeURIComponent(email)}`,
      { headers: { "X-Seal-Token": SEAL_TOKEN, "Content-Type": "application/json" } }
    );

    const subsData = await subsResponse.json();
    const subs = subsData.payload?.subscriptions || [];
    const enrollments = [];
    const now = new Date();

    for (const sub of subs) {
      const detailResponse = await fetch(
        `https://app.sealsubscriptions.com/shopify/merchant/api/subscription?id=${sub.id}`,
        { headers: { "X-Seal-Token": SEAL_TOKEN, "Content-Type": "application/json" } }
      );

      if (!detailResponse.ok) continue;
      const detailData = await detailResponse.json();
      const detail = detailData.payload;
      if (!detail.items || detail.items.length === 0) continue;

      const item = detail.items[0];
      const props = item.properties || [];
      const getProp = key => props.find(p => p.key === key)?.value || "";

      const billingAttempts = detail.billing_attempts || [];
      const nextAttempt = billingAttempts.find(a => new Date(a.date) >= now) || null;

      const previousPayments = billingAttempts
        .filter(a => new Date(a.date) < now)
        .slice(-4)
        .map(a => ({ date: a.date, amount: item.price ? `$${item.price}` : "", status: a.status || "unknown" }));

      enrollments.push({
        subscription_id: sub.id,
        child_first_name: getProp("Child First Name"),
        child_last_name: getProp("Child Last Name"),
        cricclub_id: getProp("Child CricClub ID"),
        program: getProp("Program Level") || item.title || "",
        payment_frequency: getProp("Billing Interval") || sub.billing_interval || "",
        next_payment_date: nextAttempt?.date || "",
        next_payment_amount: item.price ? `$${item.price}` : "",
        parent_email: email,
        previous_payments: previousPayments
      });
    }

    res.json({ success: true, enrollments });
  } catch (err) {
    console.error("Error fetching enrollments:", err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// Fetch vacations
app.get('/vacations', async (req, res) => {
  const { customer_id, child_name } = req.query;
  if (!customer_id || !child_name) return res.status(400).json({ success: false, error: 'Missing parameters' });

  try {
    const { rows } = await pool.query(
      `SELECT from_date, to_date, shift_days, reason
       FROM vacation_requests
       WHERE customer_id=$1 AND child_name=$2
       ORDER BY from_date DESC`,
      [customer_id, child_name]
    );
    res.json({ success: true, vacations: rows });
  } catch (err) {
    console.error(err);
    res.status(500).json({ success: false, error: 'Database error' });
  }
});

// Submit vacation request with overlap check
app.post('/vacation-request', async (req, res) => {
  const { customer_id, child_name, from_date, to_date, shift_days, reason, subscription_id, billing_attempt_id } = req.body;

  if (!customer_id || !child_name || !from_date || !to_date || !shift_days || !subscription_id || !billing_attempt_id) {
    return res.status(400).json({ success: false, error: 'Missing required fields' });
  }

  try {
    // Check for overlapping vacations
    const { rows } = await pool.query(
      `SELECT * FROM vacation_requests
       WHERE customer_id=$1 AND child_name=$2
         AND (daterange(from_date, to_date, '[]') && daterange($3::date, $4::date, '[]'))`,
      [customer_id, child_name, from_date, to_date]
    );

    if (rows.length > 0) {
      return res.status(409).json({ success: false, error: `You already have a vacation from ${rows[0].from_date} to ${rows[0].to_date}` });
    }

    // Insert new vacation
    const insert = await pool.query(
      `INSERT INTO vacation_requests
       (customer_id, child_name, from_date, to_date, shift_days, reason, subscription_id, billing_attempt_id)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *`,
      [customer_id, child_name, from_date, to_date, shift_days, reason, subscription_id, billing_attempt_id]
    );

    res.json({ success: true, vacation: insert.rows[0] });
  } catch (err) {
    console.error(err);
    res.status(500).json({ success: false, error: 'Internal server error' });
  }
});

app.listen(PORT, () => {
  console.log(`Server listening on http://localhost:${PORT}`);
});
