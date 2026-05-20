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

const requireAdminKey = (req, res) => {
  const adminKey = req.query.key || req.body?.key;
  if (!process.env.ADMIN_REPORT_KEY || adminKey !== process.env.ADMIN_REPORT_KEY) {
    res.status(403).json({ success: false, error: 'Unauthorized' });
    return false;
  }

  return true;
};

const getDateStringInTimeZone = (date, timeZone = 'America/New_York') => {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit'
  }).formatToParts(date);

  const values = Object.fromEntries(parts.map(part => [part.type, part.value]));
  return `${values.year}-${values.month}-${values.day}`;
};

const normalizeProductName = (value = '') =>
  String(value).trim().toLowerCase().replace(/[^a-z0-9]+/g, ' ');

const findSubscriptionItemByProduct = (subscription, normalizedProductName) =>
  (subscription.items || []).find(item =>
    normalizeProductName(item.title || '') === normalizedProductName
  ) || null;

const addDaysToDateString = (dateValue, days) => {
  const date = new Date(dateValue);
  date.setDate(date.getDate() + Number(days));
  return getDateStringInTimeZone(date, 'UTC');
};

const daysBetweenInclusive = (fromDate, toDate) => {
  const from = new Date(`${fromDate}T00:00:00Z`);
  const to = new Date(`${toDate}T00:00:00Z`);
  return Math.floor((to - from) / (1000 * 60 * 60 * 24)) + 1;
};

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

const createBulkCreditTableSQL = `
CREATE TABLE IF NOT EXISTS bulk_credit_requests (
  id SERIAL PRIMARY KEY,
  product_title TEXT NOT NULL,
  from_date DATE,
  to_date DATE,
  credit_days INT NOT NULL,
  dry_run BOOLEAN NOT NULL DEFAULT true,
  matched_count INT NOT NULL DEFAULT 0,
  updated_count INT NOT NULL DEFAULT 0,
  failed_count INT NOT NULL DEFAULT 0,
  results JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
`;
pool.query(createBulkCreditTableSQL)
  .then(() => console.log('bulk_credit_requests table ready'))
  .catch(err => console.error('Error creating bulk_credit_requests table:', err));

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

async function fetchActiveSealSubscriptions() {
  let page = 1;
  let hasMore = true;
  const subscriptions = [];

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
      throw new Error(`Seal subscriptions fetch failed: ${errText}`);
    }

    const data = await apiRes.json();
    const batch = data.payload?.subscriptions || [];
    subscriptions.push(...batch.filter(isActiveSubscription));

    hasMore = batch.length >= 50;
    page++;
  }

  return subscriptions;
}

async function fetchSealSubscriptionDetail(subscriptionId) {
  const detailRes = await fetch(
    `https://app.sealsubscriptions.com/shopify/merchant/api/subscription?id=${encodeURIComponent(subscriptionId)}`,
    {
      headers: {
        'X-Seal-Token': SEAL_TOKEN,
        'Content-Type': 'application/json'
      }
    }
  );

  if (!detailRes.ok) {
    const errText = await detailRes.text();
    throw new Error(`Seal subscription detail fetch failed for ${subscriptionId}: ${errText}`);
  }

  const detailData = await detailRes.json();
  return detailData.payload || {};
}

function getNextUnpaidBillingAttempt(billingAttempts = []) {
  const now = new Date();

  return billingAttempts
    .filter(attempt => {
      const status = (attempt.status || '').toLowerCase();
      return attempt.date && status !== 'completed' && new Date(attempt.date) >= now;
    })
    .sort((a, b) => new Date(a.date) - new Date(b.date))[0] || null;
}

async function rescheduleSealBillingAttempt({ billingAttemptId, subscriptionId, date }) {
  const sealRes = await fetch('https://app.sealsubscriptions.com/shopify/merchant/api/subscription-billing-attempt', {
    method: 'PUT',
    headers: { 'X-Seal-Token': SEAL_TOKEN, 'Content-Type': 'application/json' },
    body: JSON.stringify({
      id: billingAttemptId,
      subscription_id: subscriptionId,
      date,
      time: "14:30",
      timezone: "-04:00",
      action: "reschedule",
      reset_schedule: "true"
    })
  });

  let result = {};
  try {
    result = await sealRes.json();
  } catch (err) {
    result = {};
  }

  if (!sealRes.ok) {
    throw new Error(result.error || `Seal reschedule failed with status ${sealRes.status}`);
  }

  return result;
}

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

// -------------------- Admin Outdoor Warminster Participants Report --------------------
app.get('/admin/outdoor-warminster-participants', async (req, res) => {
  const adminKey = req.query.key;
  const itemNames = [
    'Outdoor Coaching - Warminster',
    'Outdoor Coaching Phoenixville'
  ];
  const normalizeItemName = (value = '') =>
    value.trim().toLowerCase().replace(/[^a-z0-9]+/g, ' ');
  const itemNameTokens = itemNames.map(normalizeItemName);

  if (adminKey !== process.env.ADMIN_REPORT_KEY) {
    return res.status(403).send("Unauthorized");
  }

  const escapeCsv = (value = '') =>
    `"${String(value ?? '').replace(/"/g, '""')}"`;

  const getProp = (properties, key) => {
    const searchKey = key.trim().toLowerCase();

    for (const p of properties || []) {
      const normalized = (p.key || '').replace(/:+$/, '').trim().toLowerCase();
      if (normalized === searchKey) return p.value || '';
    }

    return '';
  };

  const subscriptionHasItem = (subscription) =>
    (subscription.items || []).some(item =>
      itemNameTokens.includes(normalizeItemName(item.title))
    );

  try {
    let page = 1;
    let hasMore = true;
    const subscriptions = [];

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

      subscriptions.push(...subs);

      hasMore = subs.length > 0;
      page++;
    }

    const rows = [['Item Name', 'Participant Name']];

    for (const sub of subscriptions) {
      if (Array.isArray(sub.items) && sub.items.length > 0 && !subscriptionHasItem(sub)) {
        continue;
      }

      const detailRes = await fetch(
        `https://app.sealsubscriptions.com/shopify/merchant/api/subscription?id=${encodeURIComponent(sub.id)}`,
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
      const item = (detail.items || []).find(i =>
        itemNameTokens.includes(normalizeItemName(i.title))
      );
      if (!item) continue;

      rows.push([
        item.title || '',
        getProp(item.properties, 'Participant Name')
      ]);
    }

    const [header, ...participantRows] = rows;
    participantRows.sort((a, b) =>
      (a[0] || '').localeCompare(b[0] || '', undefined, { sensitivity: 'base' }) ||
      (a[1] || '').localeCompare(b[1] || '', undefined, { sensitivity: 'base' })
    );

    const csv = [header, ...participantRows].map(row => row.map(escapeCsv).join(',')).join('\n') + '\n';

    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', 'attachment; filename=outdoor_coaching_participants.csv');
    res.send(csv);
  } catch (err) {
    console.error('Outdoor Warminster participants report error:', err);
    res.status(500).json({ error: 'Failed to generate Outdoor Warminster participants report' });
  }
});

// -------------------- Admin Product List for Bulk Credits --------------------
app.get('/admin/subscription-products', async (req, res) => {
  if (!requireAdminKey(req, res)) return;

  try {
    const subscriptions = await fetchActiveSealSubscriptions();
    const productMap = new Map();

    for (const sub of subscriptions) {
      try {
        const listItems = sub.items || [];
        const items = listItems.length
          ? listItems
          : (await fetchSealSubscriptionDetail(sub.id)).items || [];

        for (const item of items) {
          const productTitle = (item.title || '').trim();
          if (!productTitle) continue;

          const normalized = normalizeProductName(productTitle);
          const current = productMap.get(normalized) || {
            product_title: productTitle,
            active_subscription_count: 0
          };

          current.active_subscription_count += 1;
          productMap.set(normalized, current);
        }
      } catch (err) {
        console.error('Error reading subscription product for', sub.id, err);
      }
    }

    const products = Array.from(productMap.values()).sort((a, b) =>
      a.product_title.localeCompare(b.product_title, undefined, { sensitivity: 'base' })
    );

    return res.json({ success: true, products });
  } catch (err) {
    console.error('Admin subscription products error:', err);
    return res.status(500).json({ success: false, error: 'Failed to load subscription products.' });
  }
});

// -------------------- Admin Bulk Holiday/Coaching Credit --------------------
app.post('/admin/bulk-credit', async (req, res) => {
  if (!requireAdminKey(req, res)) return;

  const productTitle = (req.body.product_title || '').trim();
  const fromDate = req.body.from_date || null;
  const toDate = req.body.to_date || null;
  const dryRun = req.body.dry_run !== false;
  let creditDays = Number(req.body.credit_days || 0);

  if (!productTitle) {
    return res.status(400).json({ success: false, error: 'Product is required.' });
  }

  if ((!creditDays || Number.isNaN(creditDays)) && fromDate && toDate) {
    creditDays = daysBetweenInclusive(fromDate, toDate);
  }

  if (!Number.isInteger(creditDays) || creditDays < 1 || creditDays > 365) {
    return res.status(400).json({
      success: false,
      error: 'Credit days must be a whole number between 1 and 365.'
    });
  }

  if ((fromDate && !toDate) || (!fromDate && toDate)) {
    return res.status(400).json({
      success: false,
      error: 'Please provide both from date and to date, or provide only number of days.'
    });
  }

  if (fromDate && toDate && toDate < fromDate) {
    return res.status(400).json({
      success: false,
      error: 'To date must be after or equal to from date.'
    });
  }

  const requestedProduct = normalizeProductName(productTitle);
  const results = [];
  let matchedCount = 0;
  let updatedCount = 0;
  let failedCount = 0;

  try {
    const subscriptions = await fetchActiveSealSubscriptions();

    for (const sub of subscriptions) {
      try {
        const listMatchedItem = findSubscriptionItemByProduct(sub, requestedProduct);
        if (Array.isArray(sub.items) && sub.items.length > 0 && !listMatchedItem) {
          continue;
        }

        const detail = await fetchSealSubscriptionDetail(sub.id);
        if (!isActiveSubscription({ status: detail.status || sub.status })) continue;

        const matchedItem = (detail.items || []).find(item =>
          normalizeProductName(item.title || '') === requestedProduct
        ) || listMatchedItem;
        if (!matchedItem) continue;

        matchedCount += 1;

        const nextAttempt = getNextUnpaidBillingAttempt(detail.billing_attempts || []);
        if (!nextAttempt) {
          failedCount += 1;
          results.push({
            subscription_id: sub.id,
            product_title: matchedItem.title || '',
            status: 'skipped',
            error: 'No upcoming unpaid billing attempt found.'
          });
          continue;
        }

        if (!nextAttempt.id) {
          failedCount += 1;
          results.push({
            subscription_id: sub.id,
            product_title: matchedItem.title || '',
            status: 'skipped',
            error: 'Upcoming billing attempt is missing an ID.'
          });
          continue;
        }

        const newPaymentDate = addDaysToDateString(nextAttempt.date, creditDays);
        const result = {
          subscription_id: sub.id,
          product_title: matchedItem.title || '',
          email: detail.email || '',
          billing_attempt_id: nextAttempt.id || null,
          original_payment_date: nextAttempt.date,
          new_payment_date: newPaymentDate,
          credit_days: creditDays,
          status: dryRun ? 'preview' : 'updated'
        };

        if (!dryRun) {
          await rescheduleSealBillingAttempt({
            billingAttemptId: nextAttempt.id,
            subscriptionId: sub.id,
            date: newPaymentDate
          });
          updatedCount += 1;
        }

        results.push(result);
      } catch (err) {
        failedCount += 1;
        results.push({
          subscription_id: sub.id,
          status: 'failed',
          error: err.message || 'Failed to process subscription.'
        });
      }
    }

    const insertSql = `
      INSERT INTO bulk_credit_requests
      (product_title, from_date, to_date, credit_days, dry_run, matched_count, updated_count, failed_count, results)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb)
      RETURNING id
    `;
    const insertParams = [
      productTitle,
      fromDate,
      toDate,
      creditDays,
      dryRun,
      matchedCount,
      updatedCount,
      failedCount,
      JSON.stringify(results)
    ];
    const insertResult = await pool.query(insertSql, insertParams);

    return res.json({
      success: true,
      dry_run: dryRun,
      bulk_credit_id: insertResult.rows[0].id,
      product_title: productTitle,
      from_date: fromDate,
      to_date: toDate,
      credit_days: creditDays,
      matched_count: matchedCount,
      updated_count: updatedCount,
      failed_count: failedCount,
      results
    });
  } catch (err) {
    console.error('Admin bulk credit error:', err);
    return res.status(500).json({ success: false, error: 'Failed to process bulk credit.' });
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
          product_title: item.title || "",
          participant_name: getProp('Participant Name') || `${getProp('Child First Name')} ${getProp('Child Last Name')}`.trim(),
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
    return res.status(400).json({ success: false, error: 'Please complete all required fields and try again.' });
  }

  const today = getDateStringInTimeZone(new Date());
  if (from_date <= today) {
    return res.status(400).json({
      success: false,
      error: 'Vacation start date must be a future date.'
    });
  }

  if (to_date <= from_date) {
    return res.status(400).json({
      success: false,
      error: 'Vacation end date must be after the start date.'
    });
  }

  if (Number(shift_days) < 21) {
    return res.status(400).json({
      success: false,
      error: 'Vacation requests must be at least 21 consecutive days.'
    });
  }

  try {
    // 1) Fetch subscription details from Seal
    const sealRes = await fetch(`https://app.sealsubscriptions.com/shopify/merchant/api/subscription?id=${encodeURIComponent(subscription_id)}`, {
      headers: { 'X-Seal-Token': SEAL_TOKEN, 'Content-Type': 'application/json' }
    });

    if (!sealRes.ok) {
      const errText = await sealRes.text();
      console.error('Seal API fetch subscription failed:', errText);
      return res.status(502).json({ success: false, error: 'We could not verify this subscription right now. Please try again in a few minutes.' });
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
      const billingDateLabel = firstBillingAttemptDate.toLocaleDateString('en-US', {
        month: '2-digit',
        day: '2-digit',
        year: 'numeric'
      });

      return res.status(400).json({
        success: false,
        error: `A payment is scheduled for ${billingDateLabel} before your vacation starts. Please allow that payment to complete, then submit your vacation request.`
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
      const existingFromLabel = new Date(r.from_date).toLocaleDateString('en-US', {
        month: '2-digit',
        day: '2-digit',
        year: 'numeric'
      });
      const existingToLabel = new Date(r.to_date).toLocaleDateString('en-US', {
        month: '2-digit',
        day: '2-digit',
        year: 'numeric'
      });

      return res.status(409).json({
        success: false,
        error: `There is already a vacation request from ${existingFromLabel} to ${existingToLabel}. Please choose dates that do not overlap with an existing request.`
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
    return res.status(500).json({ success: false, error: 'We could not submit your vacation request right now. Please try again in a few minutes.' });
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
  if (!requireAdminKey(req, res)) return;

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
    const escapeCsv = (value = '') =>
      `"${String(value ?? '').replace(/"/g, '""')}"`;

    let csv =
`Subscription ID,Product,Next Payment Date,Amount,Parent First Name,Parent Last Name,Parent Email,Parent Mobile,Participant Name,Age,Emergency Contact,Medical Notes,Child DOB,Program Level,Billing Interval\n`;

    const now = new Date();
    const reportRows = [];

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
        const childFirstName = getProp('Child First Name');
        const childLastName = getProp('Child Last Name');
        const childFullName = `${childFirstName || ''} ${childLastName || ''}`.replace(/\s+/g, ' ').trim();

        // 2026-05-20: Upcoming payments supports both child-based coaching subscriptions and participant-based outdoor/women's products.
        const participantName = getProp('Participant Name') || childFullName;
        const age = getProp('Age');
        const emergencyContact = getProp('Emergency Contact');
        const medicalNotes = getProp('Medical Notes');

        // =========================
        // CSV ROW
        // =========================
        reportRows.push({
          nextPaymentDate,
          nextPaymentTime: nextDateObj.getTime(),
          values: [
          sub.id,
          item.title || '',
          nextPaymentDate,
          amount,
          getProp('Parent First Name'),
          getProp('Parent Last Name'),
          getProp('Parent Email'),
          getProp('Parent Mobile'),
          participantName,
          age,
          emergencyContact,
          medicalNotes,
          getProp('Child DOB'),
          getProp('Program Level') || item.title || '',
          getProp('Billing Interval') || sub.billing_interval || ''
          ]
        });

      } catch (err) {
        console.error("Subscription error:", sub.id, err);
        continue;
      }
    }

    // 2026-05-20: Sort upcoming payment report by Next Payment Date.
    reportRows
      .sort((a, b) => a.nextPaymentTime - b.nextPaymentTime)
      .forEach(row => {
        csv += row.values.map(escapeCsv).join(',') + '\n';
      });

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
