// proxy-server.js
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

const isActiveSubscription = (subscription = {}) =>
  (subscription.status || '').toUpperCase() === 'ACTIVE';

if (!SEAL_TOKEN || !ALLOWED_ORIGIN) {
  console.error('ERROR: SEAL_TOKEN and ALLOWED_ORIGIN must be set in .env file');
  process.exit(1);
}

// Create Postgres pool (use DATABASE_URL if provided)

const pool = new Pool({
  connectionString: 'postgresql://apps:pI9skLOqhlZsVTCpUae0E4YnvgzhG7o8@dpg-d3k8q9l6ubrc73dp5afg-a.oregon-postgres.render.com/vacation_db_gdge',
  ssl: {
    rejectUnauthorized: false  // Required for Render Postgres
  }
});


// Test connection
pool.connect()
  .then(client => {
    client.release();
    console.log('Connected to Postgres');
  })
  .catch(err => {
    console.error('Postgres connection error', err);
  });

app.use(helmet());
app.use(cors({ origin: ALLOWED_ORIGIN }));
app.use(express.json());

// Initialize DB table if not exists (keeps schema similar to your SQLite original)
const createTableSQL = `
CREATE TABLE IF NOT EXISTS vacation_requests (
  id SERIAL PRIMARY KEY,
  customer_id TEXT NOT NULL,
  child_name TEXT NOT NULL,
  from_date DATE NOT NULL,
  to_date DATE NOT NULL,
  shift_days INT NOT NULL,
  reason TEXT,
  subscription_id TEXT NOT NULL,
  billing_attempt_id TEXT
);
`;
pool.query(createTableSQL)
  .then(() => console.log('vacation_requests table ready'))
  .catch(err => console.error('Error creating vacation_requests table:', err));

// Root
app.get('/', (req, res) => {
  res.send('Seal Proxy Secure Server is running! ✅');
});

// -------------------- Seal Subscriptions (by email) --------------------
app.get('/seal-subscriptions', async (req, res) => {
  const email = req.query.email;
  if (!email) return res.status(400).json({ error: "Missing 'email' query parameter" });

  try {
    const apiRes = await fetch(`https://app.sealsubscriptions.com/shopify/merchant/api/subscriptions?query=${encodeURIComponent(email)}`, {
      headers: { 'X-Seal-Token': SEAL_TOKEN, 'Content-Type': 'application/json' },
    });

    if (!apiRes.ok) {
      const errorText = await apiRes.text();
      return res.status(apiRes.status).json({ error: `Seal API error: ${apiRes.status} - ${errorText}` });
    }

    const data = await apiRes.json();
    // 2026-04-25: Keep legacy subscription lookup active-only for vacation dropdown compatibility.
    const activeSubscriptions = (data.payload?.subscriptions || []).filter(isActiveSubscription);
    // keep response shape same as your old code
    res.json({ success: true, payload: { subscriptions: activeSubscriptions } });
  } catch (err) {
    console.error('Error in /seal-subscriptions:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// -------------------- Seal Subscription detail --------------------
app.get('/seal-subscription', async (req, res) => {
  const subscriptionId = req.query.id;
  if (!subscriptionId) return res.status(400).json({ error: "Missing 'id' query parameter" });

  try {
    const apiRes = await fetch(`https://app.sealsubscriptions.com/shopify/merchant/api/subscription?id=${encodeURIComponent(subscriptionId)}`, {
      headers: { 'X-Seal-Token': SEAL_TOKEN, 'Content-Type': 'application/json' },
    });

    if (!apiRes.ok) {
      const errText = await apiRes.text();
      return res.status(apiRes.status).json({ error: `Seal API error: ${apiRes.status} - ${errText}` });
    }

    const data = await apiRes.json();
    // wrap in success/payload to keep front-end expectations consistent
    return res.json({ success: true, payload: data.payload || {} });
  } catch (err) {
    console.error('Error in /seal-subscription:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// -------------------- Reschedule billing attempt (proxy to Seal) --------------------
app.put('/reschedule-billing-attempt', async (req, res) => {
  const { billing_attempt_id, subscription_id, date, time, timezone } = req.body;

  if (!billing_attempt_id || !subscription_id || !date || !time || !timezone) {
    return res.status(400).json({ error: "Missing required fields" });
  }

  try {
    const sealRes = await fetch('https://app.sealsubscriptions.com/shopify/merchant/api/subscription-billing-attempt', {
      method: 'PUT',
      headers: { 'X-Seal-Token': SEAL_TOKEN, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        id: billing_attempt_id,
        subscription_id,
        date,
        time,
        timezone,
        action: "reschedule",
        reset_schedule: "true"
      })
    });

    const result = await sealRes.json();
    if (!sealRes.ok) {
      return res.status(sealRes.status).json({ error: result.error || 'Failed to reschedule' });
    }

    return res.json({ success: true, result });
  } catch (err) {
    console.error('Error rescheduling billing:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// -------------------- Admin Seal Report (ACTIVE ONLY) --------------------
app.get('/admin/seal-report', async (req, res) => {
  const adminKey = req.query.key;

  if (adminKey !== process.env.ADMIN_REPORT_KEY) {
    return res.status(403).send("Unauthorized");
  }

  try {
    let page = 1;
    let hasMore = true;
    let allSubscriptions = [];

    while (hasMore) {
      const apiRes = await fetch(
        `https://app.sealsubscriptions.com/shopify/merchant/api/subscriptions?page=${page}`,
        {
          headers: {
            'X-Seal-Token': SEAL_TOKEN,
            'Content-Type': 'application/json'
          }
        }
      );

      if (!apiRes.ok) {
        const errText = await apiRes.text();
        throw new Error(errText);
      }

      const data = await apiRes.json();
      const subs = data.payload?.subscriptions || [];

      // ✅ FILTER ACTIVE ONLY HERE
      const activeSubs = subs.filter(isActiveSubscription);

      allSubscriptions.push(...activeSubs);

      hasMore = subs.length > 0;
      page++;
    }

    let csv = `Subscription ID,Product,Parent First Name,Parent Last Name,Parent Mobile,Parent Email,City,State,Zip,Child First Name,Child Last Name,Child DOB,CricClubID, Program Level,Billing Interval,Next Billing Date\n`;

    for (const sub of allSubscriptions) {

      const detailRes = await fetch(
        `https://app.sealsubscriptions.com/shopify/merchant/api/subscription?id=${sub.id}`,
        {
          headers: {
            'X-Seal-Token': SEAL_TOKEN,
            'Content-Type': 'application/json'
          }
        }
      );

      if (!detailRes.ok) continue;

      const detailData = await detailRes.json();
      const detail = detailData.payload || {};
      const item = detail.items?.[0];
      if (!item) continue;

      const props = item.properties || [];
      const getProp = (key) => {
  const searchKey = key.trim().toLowerCase();
  for (const p of props) {
    // Remove trailing colons (:: or :)
    const normalized = (p.key || '').replace(/:+$/, '').trim().toLowerCase();
    if (normalized === searchKey) return p.value || '';
  }
  return '';
};

      const billingAttempts = detail.billing_attempts || [];
      const now = new Date();
      const nextAttempt = billingAttempts.find(a => new Date(a.date) >= now);

      csv += `"${sub.id}","${item.title}",` +
             `"${getProp('Parent First Name')}","${getProp('Parent Last Name')}","${getProp('Parent Mobile')}","${getProp('Parent Email')}",` +
             `"${getProp('Parent City')}","${getProp('Parent State')}","${getProp('Parent Zip')}",` +
             `"${getProp('Child First Name')}","${getProp('Child Last Name')}","${getProp('Child DOB')}",` +
			 `"${getProp('Child CricClub ID')}",`+
             `"${getProp('Program Level')}","${getProp('Billing Interval')}",` +
             `"${nextAttempt?.date || ''}"\n`;
    }

    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', 'attachment; filename=active_seal_subscription_report.csv');
    res.send(csv);

  } catch (err) {
    console.error('Seal report error:', err);
    res.status(500).json({ error: 'Failed to generate report' });
  }
});

// -------------------- Enrollments (combined endpoint) --------------------
app.get('/enrollments', async (req, res) => {
  const email = req.query.email;
  if (!email) return res.status(400).json({ success: false, error: 'Missing email' });

  try {
    const subsResponse = await fetch(`https://app.sealsubscriptions.com/shopify/merchant/api/subscriptions?query=${encodeURIComponent(email)}`, {
      headers: { 'X-Seal-Token': SEAL_TOKEN, 'Content-Type': 'application/json' },
    });

    if (!subsResponse.ok) {
      const txt = await subsResponse.text();
      return res.status(subsResponse.status).json({ success: false, error: `Seal API error: ${txt}` });
    }

    const subsData = await subsResponse.json();
    // 2026-04-25: Only return active Seal subscriptions so vacation dropdowns do not show inactive enrollments.
    const subs = (subsData.payload?.subscriptions || []).filter(isActiveSubscription);
    const now = new Date();
    const enrollments = [];

    for (const sub of subs) {
      try {
        const detailResponse = await fetch(`https://app.sealsubscriptions.com/shopify/merchant/api/subscription?id=${encodeURIComponent(sub.id)}`, {
          headers: { 'X-Seal-Token': SEAL_TOKEN, 'Content-Type': 'application/json' },
        });

        if (!detailResponse.ok) {
          const errText = await detailResponse.text();
          console.error(`Seal detail fetch failed for ${sub.id}: ${errText}`);
          continue;
        }

        const detailData = await detailResponse.json();
        const detail = detailData.payload || {};
        if (!isActiveSubscription({ status: detail.status || sub.status })) continue;
        if (!detail.items || detail.items.length === 0) continue;

        const item = detail.items[0];
        const props = item.properties || [];
        const getProp = (key) => {
          const searchKey = key.trim().toLowerCase();

          for (const p of props) {
            const normalized = (p.key || '').replace(/:+$/, '').trim().toLowerCase();
            if (normalized === searchKey) return p.value || "";
          }

          return "";
        };

        const billingAttempts = detail.billing_attempts || [];
        const nextAttempt = billingAttempts.find(a => new Date(a.date) >= now) || null;

        const previousPayments = billingAttempts
          .filter(a => new Date(a.date) < now)
          .slice(-4)
          .map(a => ({
            date: a.date,
            amount: item.price ? `$${item.price}` : "",
            status: a.status || "unknown",
            id: a.id || null
          }));

        enrollments.push({
          subscription_id: sub.id,
          status: detail.status || sub.status || "",
          child_first_name: getProp('Child First Name'),
          child_last_name: getProp('Child Last Name'),
          cricclub_id: getProp('Child CricClub ID'),
          program: getProp('Program Level') || item.title || "",
          payment_frequency: getProp('Billing Interval') || sub.billing_interval || "",
          next_payment_date: nextAttempt?.date || "",
          next_billing_attempt_id: nextAttempt?.id || null,
          next_payment_amount: item.price ? `$${item.price}` : "",
          parent_email: email,
          previous_payments: previousPayments
        });
      } catch (innerErr) {
        console.error('Error processing subscription detail for', sub.id, innerErr);
        continue;
      }
    }

    // 2026-04-25: Sort active enrollments by child name for consistent dropdown ordering.
    enrollments.sort((a, b) => {
      const nameA = `${a.child_first_name || ''} ${a.child_last_name || ''}`.trim();
      const nameB = `${b.child_first_name || ''} ${b.child_last_name || ''}`.trim();
      return nameA.localeCompare(nameB, undefined, { sensitivity: 'base' });
    });

    return res.json({ success: true, enrollments });
  } catch (err) {
    console.error('Error in /enrollments:', err);
    return res.status(500).json({ success: false, error: err.message });
  }
});

// -------------------- Fetch vacations for a child --------------------
app.get('/vacations', async (req, res) => {
  const { customer_id, child_name } = req.query;
  if (!customer_id || !child_name) {
    return res.status(400).json({ success: false, error: 'Missing parameters' });
  }

  try {
    const sql = `
      SELECT from_date::text, to_date::text, shift_days, reason
      FROM vacation_requests
      WHERE customer_id = $1 AND child_name = $2
      ORDER BY from_date DESC
    `;
    const { rows } = await pool.query(sql, [customer_id, child_name]);
    return res.json({ success: true, vacations: rows });
  } catch (err) {
    console.error('Error fetching vacations:', err);
    return res.status(500).json({ success: false, error: 'Database error' });
  }
});

// SG Mail
const sgMail = require('@sendgrid/mail');
sgMail.setApiKey(process.env.SENDGRID_API_KEY);

async function sendVacationConfirmationEmail(toEmail, updatedBillingAttempts) {
  const rowsHtml = updatedBillingAttempts.map(attempt => `
    <tr>
      <td>${attempt.original_date.slice(0,10)}</td>
      <td>${attempt.date.slice(0,10)}</td>
    </tr>
  `).join('');

  const html = `
    <p>Hi,</p>
    <p>Your vacation request has been processed. Here are the updated billing attempts:</p>
    <table border="1" cellpadding="5" cellspacing="0">
      <tr>
        <th>Original Date</th>
        <th>New Date</th>
      </tr>
      ${rowsHtml}
    </table>
    <p>Thank you,<br>MLCA Coaching</p>
  `;

  const msg = {
    to: toEmail,
    from: process.env.EMAIL_FROM,
    subject: 'Vacation Request Confirmation',
    html,
  };

  await sgMail.send(msg);
  console.log('Vacation confirmation email sent to', toEmail);
}


// -------------------- Submit vacation request (with overlap check + paid schedule validation) --------------------
app.post('/vacation-request', async (req, res) => {
  const {
    customer_id,
    child_name,
    from_date,
    to_date,
    shift_days,
    reason,
    subscription_id,
    billing_attempt_id
  } = req.body;

  if (!customer_id || !child_name || !from_date || !to_date || !shift_days || !subscription_id || (billing_attempt_id === undefined)) {
    return res.status(400).json({ success: false, error: 'Missing required fields' });
  }

  try {
    // 1) Fetch subscription details from Seal
    const sealRes = await fetch(`https://app.sealsubscriptions.com/shopify/merchant/api/subscription?id=${encodeURIComponent(subscription_id)}`, {
      headers: { 'X-Seal-Token': SEAL_TOKEN, 'Content-Type': 'application/json' }
    });

    if (!sealRes.ok) {
      const errText = await sealRes.text();
      console.error('Seal API fetch subscription failed:', errText);
      return res.status(502).json({ success: false, error: 'Failed to fetch subscription details from Seal' });
    }

    const subscriptionData = await sealRes.json();
    const billingAttempts = subscriptionData.payload?.billing_attempts || [];
	const customerEmail = subscriptionData.payload?.email;

    // 2) Compute firstBillingAttemptDate based on unpaid/future attempts
    let firstBillingAttemptDate = null;
    const now = new Date();
    if (billingAttempts.length > 0) {
      const nextAttempt = billingAttempts.find(attempt => {
        const attemptDate = new Date(attempt.date);
        return (!attempt.status || attempt.status.toLowerCase() !== 'completed') && attemptDate >= now;
      });
      firstBillingAttemptDate = nextAttempt ? new Date(nextAttempt.date) : new Date(billingAttempts[0].date);
    }

    if (firstBillingAttemptDate && new Date(from_date) > firstBillingAttemptDate) {
      return res.status(400).json({
        success: false,
        error: `You cannot submit this request now. Please submit it on or before ${firstBillingAttemptDate.toISOString().slice(0,10)}`
      });
    }

    // 3) Check for overlapping vacations
    const overlapSql = `
      SELECT * FROM vacation_requests
      WHERE customer_id = $1
        AND child_name = $2
        AND (
          ($3::date <= to_date AND $3::date >= from_date)
          OR
          ($4::date <= to_date AND $4::date >= from_date)
          OR
          (from_date <= $5::date AND to_date >= $6::date)
        )
      LIMIT 1
    `;
    const overlapParams = [customer_id, child_name, from_date, to_date, from_date, to_date];
    const overlapRes = await pool.query(overlapSql, overlapParams);
    if (overlapRes.rows.length > 0) {
      const r = overlapRes.rows[0];
      return res.status(409).json({
        success: false,
        error: `You have already submitted a vacation request from ${r.from_date.toISOString().slice(0,10)} to ${r.to_date.toISOString().slice(0,10)}. Overlapping requests are not allowed.`
      });
    }

    // 4) Insert vacation request
    const insertSql = `
      INSERT INTO vacation_requests
      (customer_id, child_name, from_date, to_date, shift_days, reason, subscription_id, billing_attempt_id, email_sent,email_id)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'N',$9)
      RETURNING id, customer_id, child_name, from_date::text, to_date::text, shift_days, reason, subscription_id, billing_attempt_id,email_id
    `;
    const insertParams = [customer_id, child_name, from_date, to_date, shift_days, reason || null, subscription_id, billing_attempt_id || null,customerEmail];
    const insertResult = await pool.query(insertSql, insertParams);

    // 5) Prepare updated billing attempts (shifted by shift_days) for confirmation
    const updatedBillingAttempts = billingAttempts.map(attempt => {
      const origDate = new Date(attempt.date);
      const newDate = new Date(origDate);
      newDate.setDate(newDate.getDate() + Number(shift_days));
      return { ...attempt, original_date: attempt.date, date: newDate.toISOString() };
    });

    

    return res.json({ 
      success: true, 
      updated: { billing_attempts: updatedBillingAttempts }, 
      id: insertResult.rows[0].id 
    });

  } catch (err) {
    console.error('Error submitting vacation request:', err);
    return res.status(500).json({ success: false, error: 'Internal server error' });
  }
});



// -------------------- Modify vacation (adjust billing if active) --------------------
app.post('/vacation-modify', async (req, res) => {
  const { vacation_id, new_to_date } = req.body;

  if (!vacation_id || !new_to_date) {
    return res.status(400).json({ success: false, error: 'Missing required fields' });
  }

  try {
    // Fetch the vacation record
    const { rows } = await pool.query('SELECT * FROM vacation_requests WHERE id = $1', [vacation_id]);
    if (rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Vacation not found' });
    }

    const vacation = rows[0];
    const today = new Date();
    const start = new Date(vacation.from_date);
    const end = new Date(vacation.to_date);
    const newEnd = new Date(new_to_date);

    // Rule: can modify only if vacation is ongoing (today between from/to)
    if (!(today >= start && today <= end)) {
      return res.status(400).json({
        success: false,
        error: 'Vacation cannot be modified because it is not currently active.',
      });
    }

    // Compute change in duration (days)
    const oldDays = Math.ceil((end - start) / (1000 * 60 * 60 * 24));
    const newDays = Math.ceil((newEnd - start) / (1000 * 60 * 60 * 24));
    const changeInDays = newDays - oldDays;

    // Update vacation record
    await pool.query(
      'UPDATE vacation_requests SET to_date = $1 WHERE id = $2',
      [new_to_date, vacation_id]
    );

    // Adjust billing via your existing Seal reschedule endpoint
    const shiftResponse = await adjustBilling(
      vacation.subscription_id,
      changeInDays
    );

    res.json({
      success: true,
      message: `Vacation updated successfully. Shifted billing by ${changeInDays} days.`,
      shift_days: changeInDays,
      seal_response: shiftResponse,
    });
  } catch (err) {
    console.error('Error modifying vacation:', err);
    res.status(500).json({ success: false, error: 'Internal server error' });
  }
});

// Helper function to adjust billing schedule in Seal
async function adjustBilling(subscriptionId, shiftDays) {
  try {
    const response = await fetch(
      'https://app.sealsubscriptions.com/shopify/merchant/api/subscription-billing-attempt',
      {
        method: 'PUT',
        headers: {
          'X-Seal-Token': SEAL_TOKEN,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          subscription_id: subscriptionId,
          shift_days: shiftDays,
          action: 'reschedule',
          reset_schedule: "true",
        }),
      }
    );

    return await response.json();
  } catch (err) {
    console.error('Billing adjustment error:', err);
    return { success: false, error: 'Failed to update billing schedule' };
  }
}

// -------------------- Seal Upcoming Payments CSV (ROBUST FULL VERSION) --------------------
app.get('/seal-upcoming-payments', async (req, res) => {
  try {

    const month = req.query.month; // YYYY-MM
    if (!month) {
      return res.status(400).send("Month required. Format: YYYY-MM");
    }

    const startDate = new Date(`${month}-01`);
    const endDate = new Date(startDate);
    endDate.setMonth(endDate.getMonth() + 1);

    // =========================
    // STEP 1: GET ALL SUBSCRIPTIONS
    // =========================
    let page = 1;
    let hasMore = true;
    let subs = [];

    while (hasMore) {
      const resSubs = await fetch(
        `https://app.sealsubscriptions.com/shopify/merchant/api/subscriptions?page=${page}`,
        {
          headers: {
            'X-Seal-Token': SEAL_TOKEN,
            'Content-Type': 'application/json'
          }
        }
      );

      if (!resSubs.ok) {
        const errText = await resSubs.text();
        console.error("Seal list error:", errText);
        throw new Error("Failed to fetch subscriptions");
      }

      const data = await resSubs.json();
      const batch = data.payload?.subscriptions || [];

      // ACTIVE ONLY (ALL CAPS)
      const activeBatch = batch.filter(s =>
        (s.status || '').toUpperCase() === 'ACTIVE'
      );

      subs.push(...activeBatch);

      console.log(`Page ${page} → Total: ${batch.length}, Active: ${activeBatch.length}`);

      if (batch.length === 0 || batch.length < 50) {
        hasMore = false;
      } else {
        page++;
      }
    }

    console.log("TOTAL ACTIVE SUBSCRIPTIONS:", subs.length);

    // =========================
    // CSV HEADER
    // =========================
    let csv =
`Subscription ID,Next Payment Date,Amount,Parent First Name,Parent Last Name,Parent Email,Parent Mobile,Child First Name,Child Last Name,Child DOB,Program Level,Billing Interval\n`;

    const now = new Date();

    // =========================
    // STEP 2: DETAILS LOOP
    // =========================
    for (const sub of subs) {

      try {

        const detailRes = await fetch(
          `https://app.sealsubscriptions.com/shopify/merchant/api/subscription?id=${sub.id}`,
          {
            headers: {
              'X-Seal-Token': SEAL_TOKEN,
              'Content-Type': 'application/json'
            }
          }
        );

        if (!detailRes.ok) continue;

        const detailData = await detailRes.json();
        const detail = detailData.payload || {};

        const item = detail.items?.[0];
        if (!item) continue;

        const props = item.properties || [];

        // =========================
        // PROPERTY PARSER (:: SAFE)
        // =========================
        const getProp = (key) => {
          const searchKey = key.trim().toLowerCase();

          for (const p of props) {
            const normalized = (p.key || '')
              .replace(/:+$/, '')
              .trim()
              .toLowerCase();

            if (normalized === searchKey) {
              return p.value || '';
            }
          }
          return '';
        };

        // =========================
        // NEXT PAYMENT LOGIC
        // =========================
        const billingAttempts = detail.billing_attempts || [];

        const nextAttempt = billingAttempts
          .filter(a => a.date)
          .map(a => ({
            ...a,
            parsedDate: new Date(a.date)
          }))
          .filter(a => a.parsedDate >= now)
          .sort((a, b) => a.parsedDate - b.parsedDate)[0];

        if (!nextAttempt) continue;

        const nextDateObj = new Date(nextAttempt.date);

        // =========================
        // MONTH FILTER
        // =========================
        if (nextDateObj < startDate || nextDateObj >= endDate) {
          continue;
        }

        const amount = item.price ? `$${item.price}` : '';

        // =========================
        // DATE CLEANING (REMOVE TIME)
        // =========================
        const cleanDate = (iso) => {
          if (!iso) return '';
          return iso.split("T")[0];
        };

        const nextPaymentDate = cleanDate(nextAttempt.date);

        // =========================
        // CSV ROW
        // =========================
        csv += `"${sub.id}","${nextPaymentDate}","${amount}",` +
          `"${getProp('Parent First Name')}","${getProp('Parent Last Name')}","${getProp('Parent Email')}","${getProp('Parent Mobile')}",` +
          `"${getProp('Child First Name')}","${getProp('Child Last Name')}","${getProp('Child DOB')}",` +
          `"${getProp('Program Level')}","${getProp('Billing Interval')}"\n`;

      } catch (err) {
        console.error("Subscription error:", sub.id, err);
        continue;
      }
    }

    // =========================
    // OUTPUT CSV
    // =========================
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader(
      'Content-Disposition',
      `attachment; filename=seal_upcoming_${month}.csv`
    );

    res.send(csv);

  } catch (err) {
    console.error("Upcoming payment error:", err);
    res.status(500).send("Failed to generate report");
  }
});

// Start server
app.listen(PORT, () => {
  console.log(`Proxy server listening on http://localhost:${PORT}`);
});
