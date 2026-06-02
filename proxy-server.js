// proxy-server.js
require('dotenv').config();
const express = require('express');
const fetch = require('node-fetch');
const cors = require('cors');
const helmet = require('helmet');
const { Pool } = require('pg');
const crypto = require('crypto');

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

const normalizeSearchText = (value = '') =>
  String(value).trim().toLowerCase().replace(/\s+/g, ' ');

const findSubscriptionItemByProduct = (subscription, normalizedProductName) =>
  (subscription.items || []).find(item =>
    normalizeProductName(item.title || '') === normalizedProductName
  ) || null;

const getItemProperty = (properties = [], key) => {
  const searchKey = key.trim().toLowerCase();

  for (const p of properties || []) {
    const normalized = (p.key || '').replace(/:+$/, '').trim().toLowerCase();
    if (normalized === searchKey) return p.value || '';
  }

  return '';
};

const escapeCsv = (value = '') =>
  `"${String(value ?? '').replace(/"/g, '""')}"`;

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

const previousDateString = (dateValue) => addDaysToDateString(dateValue, -1);

const parseBoolean = (value) =>
  value === true || value === 'true' || value === '1' || value === 1 || value === 'on';

const dayNames = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];

const getNextDatesForDay = (dayName, count = 6) => {
  const targetIndex = dayNames.findIndex(day => day.toLowerCase() === String(dayName || '').toLowerCase());
  if (targetIndex < 0) return [];

  const dates = [];
  const cursor = new Date();
  cursor.setHours(0, 0, 0, 0);

  while (dates.length < count) {
    if (cursor.getDay() === targetIndex) {
      dates.push(getDateStringInTimeZone(cursor, 'UTC'));
    }
    cursor.setDate(cursor.getDate() + 1);
  }

  return dates;
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

const handleShopifyOrderWebhook = async (req, res) => {
  try {
    if (!isValidShopifyWebhook(req)) {
      return res.status(401).json({ success: false, error: 'Invalid webhook signature' });
    }

    const order = JSON.parse(req.body.toString('utf8'));
    const waiverResult = await finalizeCoachingWaiversForPaidOrder(order);
    const trialResult = await finalizeTrialSessionsForPaidOrder(order);
    return res.json({ success: true, waivers: waiverResult, trials: trialResult });
  } catch (err) {
    console.error('Error processing Shopify order paid webhook:', err);
    return res.status(500).json({ success: false, error: 'Webhook processing failed' });
  }
};

app.post('/shopify/order-paid', express.raw({ type: 'application/json' }), handleShopifyOrderWebhook);
app.post('/shopify/order-created', express.raw({ type: 'application/json' }), handleShopifyOrderWebhook);
app.use(express.json({ limit: '8mb' }));

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

const createLocationsTableSQL = `
CREATE TABLE IF NOT EXISTS locations (
  location_id SERIAL PRIMARY KEY,
  location_name TEXT NOT NULL,
  address_1 TEXT NOT NULL,
  address_2 TEXT,
  city TEXT NOT NULL,
  state TEXT NOT NULL,
  zip TEXT NOT NULL,
  country TEXT NOT NULL
);
`;
pool.query(createLocationsTableSQL)
  .then(() => console.log('locations table ready'))
  .catch(err => console.error('Error creating locations table:', err));

const createBatchesTableSQL = `
CREATE TABLE IF NOT EXISTS batches (
  batch_id SERIAL PRIMARY KEY,
  location_id INT,
  batch_name TEXT NOT NULL,
  day TEXT NOT NULL,
  time TEXT NOT NULL,
  end_time TEXT,
  comments TEXT
);
`;
pool.query(createBatchesTableSQL)
  .then(() => console.log('batches table ready'))
  .catch(err => console.error('Error creating batches table:', err));

pool.query('ALTER TABLE batches ADD COLUMN IF NOT EXISTS end_time TEXT')
  .then(() => console.log('batches end_time column ready'))
  .catch(err => console.error('Error creating batches end_time column:', err));

pool.query('ALTER TABLE batches ADD COLUMN IF NOT EXISTS location_id INT')
  .then(() => console.log('batches location_id column ready'))
  .catch(err => console.error('Error creating batches location_id column:', err));

pool.query('ALTER TABLE locations ADD COLUMN IF NOT EXISTS is_indoor BOOLEAN NOT NULL DEFAULT false')
  .then(() => console.log('locations is_indoor column ready'))
  .catch(err => console.error('Error creating locations is_indoor column:', err));

pool.query('ALTER TABLE batches ADD COLUMN IF NOT EXISTS is_trial_batch BOOLEAN NOT NULL DEFAULT false')
  .then(() => console.log('batches is_trial_batch column ready'))
  .catch(err => console.error('Error creating batches is_trial_batch column:', err));

pool.query(`
  DO $$
  BEGIN
    IF NOT EXISTS (
      SELECT 1
      FROM pg_constraint
      WHERE conname = 'batches_location_id_fkey'
    ) THEN
      ALTER TABLE batches
      ADD CONSTRAINT batches_location_id_fkey
      FOREIGN KEY (location_id) REFERENCES locations(location_id);
    END IF;
  END $$;
`)
  .then(() => console.log('batches location foreign key ready'))
  .catch(err => console.error('Error creating batches location foreign key:', err));

const createBatchAssignmentTableSQL = `
CREATE TABLE IF NOT EXISTS batch_assignment (
  id SERIAL PRIMARY KEY,
  batch_id INT NOT NULL REFERENCES batches(batch_id),
  subscription_id TEXT NOT NULL,
  from_date DATE NOT NULL,
  to_date DATE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
`;
pool.query(createBatchAssignmentTableSQL)
  .then(() => console.log('batch_assignment table ready'))
  .catch(err => console.error('Error creating batch_assignment table:', err));

pool.query('CREATE INDEX IF NOT EXISTS idx_batch_assignment_subscription_dates ON batch_assignment(subscription_id, from_date, to_date)')
  .then(() => console.log('batch_assignment subscription/date index ready'))
  .catch(err => console.error('Error creating batch_assignment subscription/date index:', err));

pool.query('CREATE INDEX IF NOT EXISTS idx_batch_assignment_batch_id ON batch_assignment(batch_id)')
  .then(() => console.log('batch_assignment batch_id index ready'))
  .catch(err => console.error('Error creating batch_assignment batch_id index:', err));

const createCoachingWaiversTableSQL = `
CREATE TABLE IF NOT EXISTS coaching_waivers (
  waiver_id TEXT PRIMARY KEY,
  subscription_id TEXT,
  order_id TEXT,
  order_name TEXT,
  customer_email TEXT NOT NULL,
  parent_first_name TEXT NOT NULL,
  parent_last_name TEXT NOT NULL,
  participant_name TEXT,
  emergency_contact_name TEXT NOT NULL,
  emergency_contact_phone TEXT NOT NULL,
  client_ip TEXT,
  status TEXT NOT NULL DEFAULT 'pending_checkout',
  waiver_payload JSONB NOT NULL,
  pdf BYTEA NOT NULL,
  submitted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  emailed_at TIMESTAMPTZ,
  paid_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
`;
pool.query(createCoachingWaiversTableSQL)
  .then(() => console.log('coaching_waivers table ready'))
  .catch(err => console.error('Error creating coaching_waivers table:', err));

pool.query('CREATE INDEX IF NOT EXISTS idx_coaching_waivers_subscription_id ON coaching_waivers(subscription_id)')
  .then(() => console.log('coaching_waivers subscription index ready'))
  .catch(err => console.error('Error creating coaching_waivers subscription index:', err));

pool.query('CREATE INDEX IF NOT EXISTS idx_coaching_waivers_email_status ON coaching_waivers(customer_email, status)')
  .then(() => console.log('coaching_waivers email/status index ready'))
  .catch(err => console.error('Error creating coaching_waivers email/status index:', err));

const createTrialSessionsTableSQL = `
CREATE TABLE IF NOT EXISTS trial_sessions (
  trial_session_id TEXT PRIMARY KEY,
  status TEXT NOT NULL DEFAULT 'pending_checkout',
  converted_to_coaching BOOLEAN NOT NULL DEFAULT false,
  order_id TEXT,
  order_name TEXT,
  customer_email TEXT NOT NULL,
  parent_first_name TEXT NOT NULL,
  parent_last_name TEXT NOT NULL,
  parent_mobile TEXT NOT NULL,
  referred_by TEXT,
  participant_first_name TEXT NOT NULL,
  participant_last_name TEXT NOT NULL,
  participant_dob DATE NOT NULL,
  location_id INT REFERENCES locations(location_id),
  batch_id INT REFERENCES batches(batch_id),
  trial_date DATE NOT NULL,
  trial_time TEXT,
  trial_end_time TEXT,
  recommended_batch_ids INT[],
  coach_notes TEXT,
  customer_email_sent_at TIMESTAMPTZ,
  coach_email_sent_at TIMESTAMPTZ,
  paid_at TIMESTAMPTZ,
  converted_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
`;
pool.query(createTrialSessionsTableSQL)
  .then(() => console.log('trial_sessions table ready'))
  .catch(err => console.error('Error creating trial_sessions table:', err));

pool.query('CREATE INDEX IF NOT EXISTS idx_trial_sessions_email_status ON trial_sessions(customer_email, status, converted_to_coaching)')
  .then(() => console.log('trial_sessions email/status index ready'))
  .catch(err => console.error('Error creating trial_sessions email/status index:', err));

pool.query('CREATE INDEX IF NOT EXISTS idx_trial_sessions_batch_date ON trial_sessions(batch_id, trial_date)')
  .then(() => console.log('trial_sessions batch/date index ready'))
  .catch(err => console.error('Error creating trial_sessions batch/date index:', err));

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

function participantFromSubscriptionDetail(sub, detail) {
  const item = detail.items?.[0];
  if (!item) return null;

  const props = item.properties || [];
  const childFullName = `${getItemProperty(props, 'Child First Name')} ${getItemProperty(props, 'Child Last Name')}`.trim();
  const participantName = getItemProperty(props, 'Participant Name') || childFullName;

  return {
    subscription_id: String(sub.id),
    product_title: item.title || '',
    participant_name: participantName,
    parent_name: `${getItemProperty(props, 'Parent First Name')} ${getItemProperty(props, 'Parent Last Name')}`.trim(),
    parent_first_name: getItemProperty(props, 'Parent First Name'),
    parent_last_name: getItemProperty(props, 'Parent Last Name'),
    child_first_name: getItemProperty(props, 'Child First Name'),
    child_last_name: getItemProperty(props, 'Child Last Name'),
    dob: getItemProperty(props, 'Child DOB'),
    cricclub_id: getItemProperty(props, 'Child CricClub ID'),
    program_level: getItemProperty(props, 'Program Level'),
    parent_email: getItemProperty(props, 'Parent Email') || detail.email || '',
    parent_mobile: getItemProperty(props, 'Parent Mobile')
  };
}

async function fetchActiveParticipants() {
  const subscriptions = await fetchActiveSealSubscriptions();
  const participants = [];

  for (const sub of subscriptions) {
    try {
      const detail = await fetchSealSubscriptionDetail(sub.id);
      if (!isActiveSubscription({ status: detail.status || sub.status })) continue;

      const participant = participantFromSubscriptionDetail(sub, detail);
      if (participant) participants.push(participant);
    } catch (err) {
      console.error('Error reading participant for', sub.id, err);
    }
  }

  participants.sort((a, b) =>
    (a.participant_name || '').localeCompare(b.participant_name || '', undefined, { sensitivity: 'base' }) ||
    (a.subscription_id || '').localeCompare(b.subscription_id || '', undefined, { sensitivity: 'base' })
  );

  return participants;
}

async function getCurrentBatchAssignments(subscriptionIds = [], asOfDate = null) {
  if (!subscriptionIds.length) return new Map();

  const effectiveDate = asOfDate || getDateStringInTimeZone(new Date());
  const sql = `
    SELECT
      ba.batch_id,
      ba.subscription_id,
      ba.from_date::text,
      ba.to_date::text,
      b.location_id,
      b.batch_name,
      b.day,
      b.time,
      b.end_time,
      b.comments,
      l.location_name,
      l.address_1,
      l.address_2,
      l.city,
      l.state,
      l.zip,
      l.country
    FROM batch_assignment ba
    JOIN batches b ON b.batch_id = ba.batch_id
    LEFT JOIN locations l ON l.location_id = b.location_id
    WHERE ba.subscription_id = ANY($1::text[])
      AND ba.from_date <= $2::date
      AND (ba.to_date IS NULL OR ba.to_date >= $2::date)
    ORDER BY
      l.location_name ASC,
      CASE b.day
        WHEN 'Monday' THEN 1
        WHEN 'Tuesday' THEN 2
        WHEN 'Wednesday' THEN 3
        WHEN 'Thursday' THEN 4
        WHEN 'Friday' THEN 5
        WHEN 'Saturday' THEN 6
        WHEN 'Sunday' THEN 7
        ELSE 8
      END,
      b.time ASC,
      b.batch_name ASC
  `;
  const { rows } = await pool.query(sql, [subscriptionIds, effectiveDate]);
  const assignmentMap = new Map();

  for (const row of rows) {
    const existing = assignmentMap.get(row.subscription_id) || [];
    existing.push(row);
    assignmentMap.set(row.subscription_id, existing);
  }

  return assignmentMap;
}

function normalizeBatchIds(batchIds = []) {
  const ids = Array.isArray(batchIds) ? batchIds : [batchIds];
  return [...new Set(ids.map(id => Number(id)).filter(id => Number.isInteger(id) && id > 0))];
}

async function saveBatchAssignments({ subscriptionId, batchIds, fromDate, closeExisting }) {
  const normalizedIds = normalizeBatchIds(batchIds);
  if (!subscriptionId) throw new Error('Subscription is required.');
  if (!normalizedIds.length) throw new Error('At least one batch is required.');

  const effectiveFromDate = fromDate || getDateStringInTimeZone(new Date());
  if (!/^\d{4}-\d{2}-\d{2}$/.test(effectiveFromDate)) {
    throw new Error('From date must use YYYY-MM-DD format.');
  }

  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    const batchCheck = await client.query(
      'SELECT batch_id FROM batches WHERE batch_id = ANY($1::int[])',
      [normalizedIds]
    );
    if (batchCheck.rows.length !== normalizedIds.length) {
      throw new Error('One or more selected batches do not exist.');
    }

    if (closeExisting) {
      const closeDate = previousDateString(effectiveFromDate);
      await client.query(
        `UPDATE batch_assignment
         SET to_date = $1::date
         WHERE subscription_id = $2
           AND from_date <= $3::date
           AND (to_date IS NULL OR to_date >= $3::date)`,
        [closeDate, subscriptionId, effectiveFromDate]
      );
    } else {
      const currentCheck = await client.query(
        `SELECT id
         FROM batch_assignment
         WHERE subscription_id = $1
           AND from_date <= $2::date
           AND (to_date IS NULL OR to_date >= $2::date)
         LIMIT 1`,
        [subscriptionId, effectiveFromDate]
      );
      if (currentCheck.rows.length) {
        throw new Error('Participant already has a current batch assignment.');
      }
    }

    for (const batchId of normalizedIds) {
      await client.query(
        `INSERT INTO batch_assignment (batch_id, subscription_id, from_date, to_date)
         VALUES ($1, $2, $3::date, NULL)`,
        [batchId, subscriptionId, effectiveFromDate]
      );
    }

    await client.query('COMMIT');
    return { subscription_id: subscriptionId, batch_ids: normalizedIds, from_date: effectiveFromDate };
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
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

    const header = [
      'Subscription ID',
      'Product',
      'Participant Name',
      'Parent Name',
      'Parent Mobile',
      'Parent Email',
      'Program Level',
      'Participant DOB',
      'CricClubID',
      'Next Billing Date'
    ];
    const rows = [];

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
      const childFullName = `${getItemProperty(props, 'Child First Name')} ${getItemProperty(props, 'Child Last Name')}`.trim();
      const participantName = getItemProperty(props, 'Participant Name') || childFullName;
      const parentName = `${getItemProperty(props, 'Parent First Name')} ${getItemProperty(props, 'Parent Last Name')}`.trim();

      const billingAttempts = detail.billing_attempts || [];
      const nextAttempt = getNextUnpaidBillingAttempt(billingAttempts);

      rows.push([
        sub.id,
        item.title,
        participantName,
        parentName,
        getItemProperty(props, 'Parent Mobile'),
        getItemProperty(props, 'Parent Email') || detail.email || '',
        getItemProperty(props, 'Program Level'),
        getItemProperty(props, 'Child DOB'),
        getItemProperty(props, 'Child CricClub ID'),
        nextAttempt?.date || ''
      ]);
    }

    const csv = [header, ...rows].map(row => row.map(escapeCsv).join(',')).join('\n') + '\n';

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

// -------------------- Admin Current Vacations --------------------
app.get('/admin/current-vacations', async (req, res) => {
  if (!requireAdminKey(req, res)) return;

  try {
    const today = getDateStringInTimeZone(new Date());
    const sql = `
      SELECT
        id,
        customer_id,
        child_name,
        from_date::text,
        to_date::text,
        shift_days,
        reason,
        subscription_id,
        billing_attempt_id
      FROM vacation_requests
      WHERE from_date <= $1::date
        AND to_date >= $1::date
      ORDER BY to_date ASC, child_name ASC
    `;
    const { rows } = await pool.query(sql, [today]);

    return res.json({
      success: true,
      report_date: today,
      count: rows.length,
      vacations: rows
    });
  } catch (err) {
    console.error('Admin current vacations error:', err);
    return res.status(500).json({ success: false, error: 'Failed to load current vacations.' });
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

// -------------------- Admin Active Participants by Product Report --------------------
app.get('/admin/product-participants', async (req, res) => {
  if (!requireAdminKey(req, res)) return;

  const productTitle = (req.query.product_title || '').trim();
  if (!productTitle) {
    return res.status(400).json({ success: false, error: 'Product is required.' });
  }

  const requestedProduct = normalizeProductName(productTitle);

  try {
    const subscriptions = await fetchActiveSealSubscriptions();
    const rows = [[
      'Product',
      'Participant Name',
      'Subscription ID',
      'Parent First Name',
      'Parent Last Name',
      'Parent Email',
      'Parent Mobile',
      'Child First Name',
      'Child Last Name',
      'Child DOB',
      'CricClub ID',
      'Program Level',
      'Billing Interval',
      'Next Billing Date'
    ]];

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

        const props = matchedItem.properties || [];
        const childFullName = `${getItemProperty(props, 'Child First Name')} ${getItemProperty(props, 'Child Last Name')}`.trim();
        const participantName = getItemProperty(props, 'Participant Name') || childFullName;
        const nextAttempt = getNextUnpaidBillingAttempt(detail.billing_attempts || []);

        rows.push([
          matchedItem.title || productTitle,
          participantName,
          sub.id,
          getItemProperty(props, 'Parent First Name'),
          getItemProperty(props, 'Parent Last Name'),
          getItemProperty(props, 'Parent Email') || detail.email || '',
          getItemProperty(props, 'Parent Mobile'),
          getItemProperty(props, 'Child First Name'),
          getItemProperty(props, 'Child Last Name'),
          getItemProperty(props, 'Child DOB'),
          getItemProperty(props, 'Child CricClub ID'),
          getItemProperty(props, 'Program Level'),
          getItemProperty(props, 'Billing Interval') || sub.billing_interval || '',
          nextAttempt?.date || ''
        ]);
      } catch (err) {
        console.error('Error reading product participant for', sub.id, err);
      }
    }

    const [header, ...participantRows] = rows;
    participantRows.sort((a, b) =>
      (a[1] || '').localeCompare(b[1] || '', undefined, { sensitivity: 'base' }) ||
      (a[2] || '').localeCompare(b[2] || '', undefined, { sensitivity: 'base' })
    );

    const csv = [header, ...participantRows]
      .map(row => row.map(escapeCsv).join(','))
      .join('\n') + '\n';
    const safeProduct = normalizeProductName(productTitle).replace(/\s+/g, '_') || 'product';

    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', `attachment; filename=active_participants_${safeProduct}.csv`);
    return res.send(csv);
  } catch (err) {
    console.error('Admin product participants report error:', err);
    return res.status(500).json({ success: false, error: 'Failed to generate product participants report.' });
  }
});

// -------------------- Admin Batch Management --------------------
app.get('/admin/locations', async (req, res) => {
  if (!requireAdminKey(req, res)) return;

  try {
    const { rows } = await pool.query(
      `SELECT location_id, location_name, address_1, address_2, city, state, zip, country, is_indoor
       FROM locations
       ORDER BY location_name ASC, city ASC`
    );
    return res.json({ success: true, locations: rows });
  } catch (err) {
    console.error('Admin locations list error:', err);
    return res.status(500).json({ success: false, error: 'Failed to load locations.' });
  }
});

app.post('/admin/locations', async (req, res) => {
  if (!requireAdminKey(req, res)) return;

  const locationName = String(req.body.location_name || '').trim();
  const address1 = String(req.body.address_1 || '').trim();
  const address2 = String(req.body.address_2 || '').trim();
  const city = String(req.body.city || '').trim();
  const state = String(req.body.state || '').trim();
  const zip = String(req.body.zip || '').trim();
  const country = String(req.body.country || '').trim();
  const isIndoor = parseBoolean(req.body.is_indoor);

  if (!locationName || !address1 || !city || !state || !zip || !country) {
    return res.status(400).json({
      success: false,
      error: 'Location name, address 1, city, state, zip, and country are required.'
    });
  }

  try {
    const { rows } = await pool.query(
      `INSERT INTO locations (location_name, address_1, address_2, city, state, zip, country, is_indoor)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
       RETURNING location_id, location_name, address_1, address_2, city, state, zip, country, is_indoor`,
      [locationName, address1, address2 || null, city, state, zip, country, isIndoor]
    );

    return res.json({ success: true, location: rows[0] });
  } catch (err) {
    console.error('Admin location create error:', err);
    return res.status(500).json({ success: false, error: 'Failed to create location.' });
  }
});

app.put('/admin/locations/:locationId', async (req, res) => {
  if (!requireAdminKey(req, res)) return;

  const locationId = Number(req.params.locationId);
  const locationName = String(req.body.location_name || '').trim();
  const address1 = String(req.body.address_1 || '').trim();
  const address2 = String(req.body.address_2 || '').trim();
  const city = String(req.body.city || '').trim();
  const state = String(req.body.state || '').trim();
  const zip = String(req.body.zip || '').trim();
  const country = String(req.body.country || '').trim();
  const isIndoor = parseBoolean(req.body.is_indoor);

  if (!Number.isInteger(locationId) || locationId < 1) {
    return res.status(400).json({ success: false, error: 'Invalid location id.' });
  }

  if (!locationName || !address1 || !city || !state || !zip || !country) {
    return res.status(400).json({
      success: false,
      error: 'Location name, address 1, city, state, zip, and country are required.'
    });
  }

  try {
    const { rows } = await pool.query(
      `UPDATE locations
       SET location_name = $1,
           address_1 = $2,
           address_2 = $3,
           city = $4,
           state = $5,
           zip = $6,
           country = $7,
           is_indoor = $8
       WHERE location_id = $9
       RETURNING location_id, location_name, address_1, address_2, city, state, zip, country, is_indoor`,
      [locationName, address1, address2 || null, city, state, zip, country, isIndoor, locationId]
    );

    if (!rows.length) {
      return res.status(404).json({ success: false, error: 'Location not found.' });
    }

    return res.json({ success: true, location: rows[0] });
  } catch (err) {
    console.error('Admin location update error:', err);
    return res.status(500).json({ success: false, error: 'Failed to update location.' });
  }
});

app.get('/admin/batches', async (req, res) => {
  if (!requireAdminKey(req, res)) return;

  try {
    const { rows } = await pool.query(
      `SELECT
         b.batch_id,
         b.location_id,
         b.batch_name,
         b.day,
         b.time,
         b.end_time,
         b.is_trial_batch,
         b.comments,
         l.location_name,
         l.is_indoor
       FROM batches b
       LEFT JOIN locations l ON l.location_id = b.location_id
       ORDER BY
         l.location_name ASC,
         CASE b.day
           WHEN 'Monday' THEN 1
           WHEN 'Tuesday' THEN 2
           WHEN 'Wednesday' THEN 3
           WHEN 'Thursday' THEN 4
           WHEN 'Friday' THEN 5
           WHEN 'Saturday' THEN 6
           WHEN 'Sunday' THEN 7
           ELSE 8
         END,
         b.time ASC,
         b.batch_name ASC`
    );
    return res.json({ success: true, batches: rows });
  } catch (err) {
    console.error('Admin batches list error:', err);
    return res.status(500).json({ success: false, error: 'Failed to load batches.' });
  }
});

app.post('/admin/batches', async (req, res) => {
  if (!requireAdminKey(req, res)) return;

  const locationId = Number(req.body.location_id || 0);
  const batchName = String(req.body.batch_name || '').trim();
  const day = String(req.body.day || '').trim();
  const time = String(req.body.time || '').trim();
  const endTime = String(req.body.end_time || '').trim();
  const comments = String(req.body.comments || '').trim();
  const isTrialBatch = parseBoolean(req.body.is_trial_batch);

  if (!Number.isInteger(locationId) || locationId < 1 || !batchName || !day || !time) {
    return res.status(400).json({ success: false, error: 'Location, batch name, day, and time are required.' });
  }

  try {
    const { rows } = await pool.query(
      `INSERT INTO batches (location_id, batch_name, day, time, end_time, comments, is_trial_batch)
       VALUES ($1, $2, $3, $4, $5, $6, $7)
       RETURNING batch_id, location_id, batch_name, day, time, end_time, comments, is_trial_batch`,
      [locationId, batchName, day, time, endTime || null, comments, isTrialBatch]
    );

    return res.json({ success: true, batch: rows[0] });
  } catch (err) {
    console.error('Admin batch create error:', err);
    return res.status(500).json({ success: false, error: 'Failed to create batch.' });
  }
});

app.put('/admin/batches/:batchId', async (req, res) => {
  if (!requireAdminKey(req, res)) return;

  const batchId = Number(req.params.batchId);
  const locationId = Number(req.body.location_id || 0);
  const batchName = String(req.body.batch_name || '').trim();
  const day = String(req.body.day || '').trim();
  const time = String(req.body.time || '').trim();
  const endTime = String(req.body.end_time || '').trim();
  const comments = String(req.body.comments || '').trim();
  const isTrialBatch = parseBoolean(req.body.is_trial_batch);

  if (!Number.isInteger(batchId) || batchId < 1) {
    return res.status(400).json({ success: false, error: 'Invalid batch id.' });
  }

  if (!Number.isInteger(locationId) || locationId < 1 || !batchName || !day || !time) {
    return res.status(400).json({ success: false, error: 'Location, batch name, day, and time are required.' });
  }

  try {
    const { rows } = await pool.query(
      `UPDATE batches
       SET location_id = $1, batch_name = $2, day = $3, time = $4, end_time = $5, comments = $6, is_trial_batch = $7
       WHERE batch_id = $8
       RETURNING batch_id, location_id, batch_name, day, time, end_time, comments, is_trial_batch`,
      [locationId, batchName, day, time, endTime || null, comments, isTrialBatch, batchId]
    );

    if (!rows.length) {
      return res.status(404).json({ success: false, error: 'Batch not found.' });
    }

    return res.json({ success: true, batch: rows[0] });
  } catch (err) {
    console.error('Admin batch update error:', err);
    return res.status(500).json({ success: false, error: 'Failed to update batch.' });
  }
});

app.delete('/admin/batches/:batchId', async (req, res) => {
  if (!requireAdminKey(req, res)) return;

  const batchId = Number(req.params.batchId);
  if (!Number.isInteger(batchId) || batchId < 1) {
    return res.status(400).json({ success: false, error: 'Invalid batch id.' });
  }

  try {
    const activeAssignmentCheck = await pool.query(
      `SELECT id
       FROM batch_assignment
       WHERE batch_id = $1
         AND (to_date IS NULL OR to_date >= $2::date)
       LIMIT 1`,
      [batchId, getDateStringInTimeZone(new Date())]
    );

    if (activeAssignmentCheck.rows.length) {
      return res.status(400).json({
        success: false,
        error: 'Batch has current assignments and cannot be deleted.'
      });
    }

    const result = await pool.query('DELETE FROM batches WHERE batch_id = $1', [batchId]);
    if (!result.rowCount) {
      return res.status(404).json({ success: false, error: 'Batch not found.' });
    }

    return res.json({ success: true });
  } catch (err) {
    console.error('Admin batch delete error:', err);
    return res.status(500).json({ success: false, error: 'Failed to delete batch.' });
  }
});

app.get('/trial/locations', async (req, res) => {
  try {
    const { rows } = await pool.query(
      `
        SELECT DISTINCT
          l.location_id,
          l.location_name,
          l.address_1,
          l.address_2,
          l.city,
          l.state,
          l.zip,
          l.country
        FROM locations l
        JOIN batches b ON b.location_id = l.location_id
        WHERE l.is_indoor = true
          AND b.is_trial_batch = true
        ORDER BY l.location_name ASC, l.city ASC
      `
    );

    return res.json({ success: true, locations: rows });
  } catch (err) {
    console.error('Trial locations error:', err);
    return res.status(500).json({ success: false, error: 'Failed to load trial locations.' });
  }
});

app.get('/trial/batches', async (req, res) => {
  const locationId = Number(req.query.location_id || 0);
  if (!Number.isInteger(locationId) || locationId < 1) {
    return res.status(400).json({ success: false, error: 'Location is required.' });
  }

  try {
    const { rows } = await pool.query(
      `
        SELECT
          b.batch_id,
          b.location_id,
          b.batch_name,
          b.day,
          b.time,
          b.end_time,
          b.comments,
          l.location_name
        FROM batches b
        JOIN locations l ON l.location_id = b.location_id
        WHERE b.location_id = $1
          AND b.is_trial_batch = true
          AND l.is_indoor = true
        ORDER BY
          CASE b.day
            WHEN 'Monday' THEN 1
            WHEN 'Tuesday' THEN 2
            WHEN 'Wednesday' THEN 3
            WHEN 'Thursday' THEN 4
            WHEN 'Friday' THEN 5
            WHEN 'Saturday' THEN 6
            WHEN 'Sunday' THEN 7
            ELSE 8
          END,
          b.time ASC,
          b.batch_name ASC
      `,
      [locationId]
    );

    return res.json({
      success: true,
      batches: rows.map(row => ({
        ...row,
        available_dates: getNextDatesForDay(row.day, 6)
      }))
    });
  } catch (err) {
    console.error('Trial batches error:', err);
    return res.status(500).json({ success: false, error: 'Failed to load trial batches.' });
  }
});

app.get('/trial/pending', async (req, res) => {
  const email = String(req.query.email || '').trim();
  if (!email) return res.status(400).json({ success: false, error: 'Email is required.' });

  try {
    const { rows } = await pool.query(
      `
        SELECT
          ts.trial_session_id,
          ts.customer_email,
          ts.parent_first_name,
          ts.parent_last_name,
          ts.parent_mobile,
          ts.participant_first_name,
          ts.participant_last_name,
          ts.participant_dob::text,
          ts.location_id,
          ts.batch_id,
          ts.trial_date::text,
          ts.trial_time,
          ts.trial_end_time,
          ts.recommended_batch_ids,
          ts.coach_notes,
          b.batch_name,
          l.location_name
        FROM trial_sessions ts
        LEFT JOIN batches b ON b.batch_id = ts.batch_id
        LEFT JOIN locations l ON l.location_id = ts.location_id
        WHERE lower(ts.customer_email) = lower($1)
          AND ts.status = 'paid'
          AND ts.converted_to_coaching = false
        ORDER BY ts.trial_date DESC, ts.created_at DESC
        LIMIT 10
      `,
      [email]
    );

    const recommendedIds = [...new Set(rows.flatMap(row => row.recommended_batch_ids || []))];
    let recommendedBatches = [];
    if (recommendedIds.length) {
      const batchResult = await pool.query(
        `
          SELECT
            b.batch_id,
            b.batch_name,
            b.day,
            b.time,
            b.end_time,
            b.location_id,
            l.location_name
          FROM batches b
          LEFT JOIN locations l ON l.location_id = b.location_id
          WHERE b.batch_id = ANY($1::int[])
          ORDER BY b.batch_name ASC
        `,
        [recommendedIds]
      );
      recommendedBatches = batchResult.rows;
    }

    return res.json({ success: true, trial_sessions: rows, recommended_batches: recommendedBatches });
  } catch (err) {
    console.error('Trial pending lookup error:', err);
    return res.status(500).json({ success: false, error: 'Failed to load trial sessions.' });
  }
});

app.get('/admin/trial-sessions', async (req, res) => {
  if (!requireAdminKey(req, res)) return;

  const status = String(req.query.status || '').trim();
  const where = status ? 'WHERE ts.status = $1' : '';
  const params = status ? [status] : [];

  try {
    const { rows } = await pool.query(
      `
        SELECT
          ts.*,
          ts.participant_dob::text,
          ts.trial_date::text,
          b.batch_name,
          l.location_name
        FROM trial_sessions ts
        LEFT JOIN batches b ON b.batch_id = ts.batch_id
        LEFT JOIN locations l ON l.location_id = ts.location_id
        ${where}
        ORDER BY ts.trial_date DESC, ts.created_at DESC
        LIMIT 100
      `,
      params
    );

    return res.json({ success: true, trial_sessions: rows });
  } catch (err) {
    console.error('Admin trial sessions error:', err);
    return res.status(500).json({ success: false, error: 'Failed to load trial sessions.' });
  }
});

app.put('/coach/trial-sessions/:trialSessionId/recommendations', async (req, res) => {
  if (!requireAdminKey(req, res)) return;

  const recommendedBatchIds = (req.body.recommended_batch_ids || [])
    .map(id => Number(id))
    .filter(id => Number.isInteger(id) && id > 0)
    .slice(0, 4);
  const coachNotes = String(req.body.coach_notes || '').trim();

  try {
    const { rows } = await pool.query(
      `
        UPDATE trial_sessions
        SET recommended_batch_ids = $2::int[],
            coach_notes = $3,
            updated_at = NOW()
        WHERE trial_session_id = $1
        RETURNING trial_session_id, recommended_batch_ids, coach_notes
      `,
      [req.params.trialSessionId, recommendedBatchIds, coachNotes || null]
    );

    if (!rows.length) return res.status(404).json({ success: false, error: 'Trial session not found.' });
    return res.json({ success: true, trial_session: rows[0] });
  } catch (err) {
    console.error('Trial recommendations error:', err);
    return res.status(500).json({ success: false, error: 'Failed to save trial recommendations.' });
  }
});

app.get('/admin/trial-sessions-pending-customer-email', async (req, res) => {
  if (!requireAdminKey(req, res)) return;

  try {
    const { rows } = await pool.query(
      `
        SELECT ts.*, ts.participant_dob::text, ts.trial_date::text, b.batch_name, l.location_name
        FROM trial_sessions ts
        LEFT JOIN batches b ON b.batch_id = ts.batch_id
        LEFT JOIN locations l ON l.location_id = ts.location_id
        WHERE ts.status = 'paid'
          AND ts.customer_email_sent_at IS NULL
        ORDER BY ts.paid_at ASC NULLS LAST, ts.created_at ASC
        LIMIT 50
      `
    );
    return res.json({ success: true, trial_sessions: rows });
  } catch (err) {
    console.error('Trial customer email poll error:', err);
    return res.status(500).json({ success: false, error: 'Failed to load trial sessions pending customer email.' });
  }
});

app.get('/admin/trial-sessions-today-for-coaches', async (req, res) => {
  if (!requireAdminKey(req, res)) return;

  const today = req.query.date || getDateStringInTimeZone(new Date());

  try {
    const { rows } = await pool.query(
      `
        SELECT ts.*, ts.participant_dob::text, ts.trial_date::text, b.batch_name, l.location_name
        FROM trial_sessions ts
        LEFT JOIN batches b ON b.batch_id = ts.batch_id
        LEFT JOIN locations l ON l.location_id = ts.location_id
        WHERE ts.status = 'paid'
          AND ts.trial_date = $1::date
          AND ts.coach_email_sent_at IS NULL
        ORDER BY ts.trial_time ASC, ts.participant_first_name ASC
      `,
      [today]
    );
    return res.json({ success: true, date: today, trial_sessions: rows });
  } catch (err) {
    console.error('Trial coach email poll error:', err);
    return res.status(500).json({ success: false, error: 'Failed to load today trial sessions.' });
  }
});

app.post('/admin/trial-sessions/:trialSessionId/mark-customer-emailed', async (req, res) => {
  if (!requireAdminKey(req, res)) return;

  try {
    const { rows } = await pool.query(
      `UPDATE trial_sessions SET customer_email_sent_at = NOW(), updated_at = NOW()
       WHERE trial_session_id = $1
       RETURNING trial_session_id, customer_email_sent_at`,
      [req.params.trialSessionId]
    );
    if (!rows.length) return res.status(404).json({ success: false, error: 'Trial session not found.' });
    return res.json({ success: true, trial_session: rows[0] });
  } catch (err) {
    console.error('Trial mark customer emailed error:', err);
    return res.status(500).json({ success: false, error: 'Failed to mark customer emailed.' });
  }
});

app.post('/admin/trial-sessions/mark-coach-emailed', async (req, res) => {
  if (!requireAdminKey(req, res)) return;

  const ids = (req.body.trial_session_ids || [])
    .map(id => String(id).trim())
    .filter(Boolean);

  if (!ids.length) return res.status(400).json({ success: false, error: 'trial_session_ids are required.' });

  try {
    const { rows } = await pool.query(
      `UPDATE trial_sessions SET coach_email_sent_at = NOW(), updated_at = NOW()
       WHERE trial_session_id = ANY($1::text[])
       RETURNING trial_session_id, coach_email_sent_at`,
      [ids]
    );
    return res.json({ success: true, trial_sessions: rows });
  } catch (err) {
    console.error('Trial mark coach emailed error:', err);
    return res.status(500).json({ success: false, error: 'Failed to mark coach emailed.' });
  }
});

// -------------------- Coach Batch Assignment APIs --------------------
app.get('/coach/batches', async (req, res) => {
  if (!requireAdminKey(req, res)) return;

  try {
    const { rows } = await pool.query(
      `SELECT
         b.batch_id,
         b.location_id,
         b.batch_name,
         b.day,
         b.time,
         b.end_time,
         b.comments,
         l.location_name
       FROM batches b
       LEFT JOIN locations l ON l.location_id = b.location_id
       ORDER BY
         l.location_name ASC,
         CASE b.day
           WHEN 'Monday' THEN 1
           WHEN 'Tuesday' THEN 2
           WHEN 'Wednesday' THEN 3
           WHEN 'Thursday' THEN 4
           WHEN 'Friday' THEN 5
           WHEN 'Saturday' THEN 6
           WHEN 'Sunday' THEN 7
           ELSE 8
         END,
         b.time ASC,
         b.batch_name ASC`
    );
    return res.json({ success: true, batches: rows });
  } catch (err) {
    console.error('Coach batches error:', err);
    return res.status(500).json({ success: false, error: 'Failed to load batches.' });
  }
});

app.get('/coach/unassigned-participants', async (req, res) => {
  if (!requireAdminKey(req, res)) return;

  const asOfDate = String(req.query.as_of_date || getDateStringInTimeZone(new Date())).trim();
  const participantName = normalizeSearchText(req.query.participant_name || '');
  const cricclubId = normalizeSearchText(req.query.cricclub_id || '');

  if (!/^\d{4}-\d{2}-\d{2}$/.test(asOfDate)) {
    return res.status(400).json({ success: false, error: 'As-of date must use YYYY-MM-DD format.' });
  }

  try {
    const participants = (await fetchActiveParticipants()).filter(participant => {
      const participantMatches = !participantName || normalizeSearchText(participant.participant_name || '').includes(participantName);
      const cricclubMatches = !cricclubId || normalizeSearchText(participant.cricclub_id || '').includes(cricclubId);
      return participantMatches && cricclubMatches;
    });
    const assignmentMap = await getCurrentBatchAssignments(participants.map(p => p.subscription_id), asOfDate);
    const unassigned = participants.filter(p => !assignmentMap.has(p.subscription_id));

    return res.json({ success: true, as_of_date: asOfDate, participants: unassigned });
  } catch (err) {
    console.error('Coach unassigned participants error:', err);
    return res.status(500).json({ success: false, error: 'Failed to load unassigned participants.' });
  }
});

app.get('/coach/participants/search', async (req, res) => {
  if (!requireAdminKey(req, res)) return;

  const query = String(req.query.q || '').trim().toLowerCase();
  if (query.length < 2) {
    return res.status(400).json({ success: false, error: 'Search must be at least 2 characters.' });
  }

  try {
    const participants = await fetchActiveParticipants();
    const matches = participants.filter(participant => {
      const searchable = [
        participant.participant_name,
        participant.child_first_name,
        participant.child_last_name,
        participant.cricclub_id,
        participant.subscription_id
      ].join(' ').toLowerCase();

      return searchable.includes(query);
    });
    const assignmentMap = await getCurrentBatchAssignments(matches.map(p => p.subscription_id));

    return res.json({
      success: true,
      participants: matches.map(participant => ({
        ...participant,
        current_assignments: assignmentMap.get(participant.subscription_id) || []
      }))
    });
  } catch (err) {
    console.error('Coach participant search error:', err);
    return res.status(500).json({ success: false, error: 'Failed to search participants.' });
  }
});

app.get('/coach/participants/details-search', async (req, res) => {
  if (!requireAdminKey(req, res)) return;

  const participantName = String(req.query.participant_name || '').trim().toLowerCase();
  const parentName = String(req.query.parent_name || '').trim().toLowerCase();
  const cricclubId = String(req.query.cricclub_id || '').trim().toLowerCase();
  const batchName = String(req.query.batch_name || '').trim().toLowerCase();

  if (!participantName && !parentName && !cricclubId && !batchName) {
    return res.status(400).json({
      success: false,
      error: 'Enter participant name, parent name, CricClub ID, or batch name.'
    });
  }

  try {
    const participants = await fetchActiveParticipants();
    const assignmentMap = await getCurrentBatchAssignments(participants.map(p => p.subscription_id));
    const matches = participants.filter(participant => {
      const assignments = assignmentMap.get(participant.subscription_id) || [];
      const participantMatches = !participantName || String(participant.participant_name || '').toLowerCase().includes(participantName);
      const parentMatches = !parentName || String(participant.parent_name || '').toLowerCase().includes(parentName);
      const cricclubMatches = !cricclubId || String(participant.cricclub_id || '').toLowerCase().includes(cricclubId);
      const batchMatches = !batchName || assignments.some(assignment =>
        String(assignment.batch_name || '').toLowerCase().includes(batchName)
      );

      return participantMatches && parentMatches && cricclubMatches && batchMatches;
    });

    return res.json({
      success: true,
      participants: matches.map(participant => ({
        ...participant,
        current_assignments: assignmentMap.get(participant.subscription_id) || []
      }))
    });
  } catch (err) {
    console.error('Coach participant details search error:', err);
    return res.status(500).json({ success: false, error: 'Failed to search participants.' });
  }
});

app.get('/coach/vacation-report', async (req, res) => {
  if (!requireAdminKey(req, res)) return;

  const today = getDateStringInTimeZone(new Date());
  const fromDate = String(req.query.from_date || today).trim();
  const toDate = String(req.query.to_date || fromDate).trim();

  if ((fromDate && !/^\d{4}-\d{2}-\d{2}$/.test(fromDate)) || (toDate && !/^\d{4}-\d{2}-\d{2}$/.test(toDate))) {
    return res.status(400).json({ success: false, error: 'Dates must use YYYY-MM-DD format.' });
  }

  if (fromDate && toDate && toDate < fromDate) {
    return res.status(400).json({ success: false, error: 'To date must be after or equal to from date.' });
  }

  try {
    const params = [];
    const where = [];

    if (fromDate) {
      params.push(fromDate);
      where.push(`vr.to_date >= $${params.length}::date`);
    }

    if (toDate) {
      params.push(toDate);
      where.push(`vr.from_date <= $${params.length}::date`);
    }

    const sql = `
      SELECT
        vr.id,
        vr.customer_id,
        vr.child_name,
        vr.from_date::text,
        vr.to_date::text,
        vr.shift_days,
        vr.reason,
        vr.subscription_id,
        vr.billing_attempt_id
      FROM vacation_requests vr
      ${where.length ? `WHERE ${where.join(' AND ')}` : ''}
      ORDER BY vr.from_date DESC, vr.to_date DESC, vr.child_name ASC
    `;
    const { rows } = await pool.query(sql, params);
    const participants = await fetchActiveParticipants();
    const participantMap = new Map(participants.map(participant => [participant.subscription_id, participant]));

    return res.json({
      success: true,
      count: rows.length,
      vacations: rows.map(row => {
        const participant = participantMap.get(String(row.subscription_id)) || {};
        return {
          ...row,
          participant_name: participant.participant_name || row.child_name || '',
          parent_name: participant.parent_name || '',
          parent_mobile: participant.parent_mobile || '',
          cricclub_id: participant.cricclub_id || '',
          program_level: participant.program_level || '',
          product_title: participant.product_title || ''
        };
      })
    });
  } catch (err) {
    console.error('Coach vacation report error:', err);
    return res.status(500).json({ success: false, error: 'Failed to load vacation report.' });
  }
});

app.post('/coach/batch-assignments', async (req, res) => {
  if (!requireAdminKey(req, res)) return;

  try {
    const result = await saveBatchAssignments({
      subscriptionId: String(req.body.subscription_id || '').trim(),
      batchIds: req.body.batch_ids,
      fromDate: req.body.from_date || null,
      closeExisting: false
    });

    return res.json({ success: true, assignment: result });
  } catch (err) {
    console.error('Coach batch assignment create error:', err);
    return res.status(400).json({ success: false, error: err.message || 'Failed to save batch assignment.' });
  }
});

app.put('/coach/batch-assignments', async (req, res) => {
  if (!requireAdminKey(req, res)) return;

  try {
    const result = await saveBatchAssignments({
      subscriptionId: String(req.body.subscription_id || '').trim(),
      batchIds: req.body.batch_ids,
      fromDate: req.body.from_date || null,
      closeExisting: true
    });

    return res.json({ success: true, assignment: result });
  } catch (err) {
    console.error('Coach batch assignment update error:', err);
    return res.status(400).json({ success: false, error: err.message || 'Failed to update batch assignment.' });
  }
});

app.post('/coach/batch-assignments/bulk', async (req, res) => {
  if (!requireAdminKey(req, res)) return;

  const assignments = Array.isArray(req.body.assignments) ? req.body.assignments : [];
  const fromDate = req.body.from_date || getDateStringInTimeZone(new Date());
  const replaceExisting = req.body.replace_existing === true;
  const dryRun = req.body.dry_run === true;

  if (!assignments.length) {
    return res.status(400).json({ success: false, error: 'Assignments are required.' });
  }

  if (!/^\d{4}-\d{2}-\d{2}$/.test(fromDate)) {
    return res.status(400).json({ success: false, error: 'From date must use YYYY-MM-DD format.' });
  }

  try {
    const [participants, batchResult] = await Promise.all([
      fetchActiveParticipants(),
      pool.query('SELECT batch_id, batch_name FROM batches')
    ]);
    const batchMap = new Map();

    for (const batch of batchResult.rows) {
      const normalizedBatchName = normalizeProductName(batch.batch_name || '');
      if (!batchMap.has(normalizedBatchName)) batchMap.set(normalizedBatchName, []);
      batchMap.get(normalizedBatchName).push(batch);
    }

    const results = [];
    let assignedCount = 0;
    let failedCount = 0;

    for (const assignment of assignments) {
      const participantName = String(assignment.participant_name || '').trim();
      const productTitle = String(assignment.product_title || '').trim();
      const batchNames = [
        assignment.day1_batch || assignment.batch_day_1 || assignment.batch_1,
        assignment.day2_batch || assignment.batch_day_2 || assignment.batch_2
      ].filter(Boolean).map(value => String(value).trim());

      const result = {
        participant_name: participantName,
        product_title: productTitle,
        batch_names: batchNames,
        status: dryRun ? 'preview' : 'assigned'
      };

      try {
        if (!participantName) throw new Error('Participant name is required.');
        if (!batchNames.length) throw new Error('At least one batch is required.');

        const participantMatches = participants.filter(participant =>
          normalizeSearchText(participant.participant_name || '') === normalizeSearchText(participantName)
        );
        const productMatches = productTitle
          ? participantMatches.filter(participant =>
              normalizeProductName(participant.product_title || '') === normalizeProductName(productTitle)
            )
          : participantMatches;

        if (!productMatches.length) throw new Error('No active participant subscription matched.');
        if (productMatches.length > 1) throw new Error('Multiple active participant subscriptions matched.');

        const batchIds = [];
        for (const batchName of batchNames) {
          const matches = batchMap.get(normalizeProductName(batchName)) || [];
          if (!matches.length) throw new Error(`Batch not found: ${batchName}`);
          if (matches.length > 1) throw new Error(`Multiple batches matched: ${batchName}`);
          batchIds.push(matches[0].batch_id);
        }

        result.subscription_id = productMatches[0].subscription_id;
        result.batch_ids = batchIds;

        if (!dryRun) {
          await saveBatchAssignments({
            subscriptionId: productMatches[0].subscription_id,
            batchIds,
            fromDate,
            closeExisting: replaceExisting
          });
        }

        assignedCount += 1;
      } catch (err) {
        result.status = 'failed';
        result.error = err.message || 'Failed to assign batches.';
        failedCount += 1;
      }

      results.push(result);
    }

    return res.json({
      success: true,
      dry_run: dryRun,
      from_date: fromDate,
      replace_existing: replaceExisting,
      assigned_count: assignedCount,
      failed_count: failedCount,
      results
    });
  } catch (err) {
    console.error('Coach bulk batch assignment error:', err);
    return res.status(500).json({ success: false, error: 'Failed to process bulk batch assignments.' });
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

    const assignmentMap = await getCurrentBatchAssignments(enrollments.map(enrollment => String(enrollment.subscription_id)));
    for (const enrollment of enrollments) {
      enrollment.current_assignments = assignmentMap.get(String(enrollment.subscription_id)) || [];
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
const PDFDocument = require('pdfkit');
sgMail.setApiKey(process.env.SENDGRID_API_KEY);

const escapeHtml = (value = '') =>
  String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');

const getClientIpAddress = (req) => {
  const forwardedFor = req.headers['x-forwarded-for'];
  if (forwardedFor) return String(forwardedFor).split(',')[0].trim();
  return req.headers['x-real-ip'] || req.ip || req.socket?.remoteAddress || '';
};

const COACHING_WAIVER_TITLE = 'StarSportsUS LLC Indoor Sports Facility / Major League Cricket Academy Philadelphia';
const COACHING_WAIVER_ITEMS = [
  'This form is valid for a minimum of six months from the date of signing.',
  'I the undersigned Participant / Parent or Guardian of the Participant recognize and acknowledge that activities at the Starsportsus LLC indoor sports facility / Major League Cricket Academy Philadelphia, could involve serious risk of injury or possibly accidental death.',
  'I understand that there may be unforeseeable risks and such risks shall be assumed and are understood by the undersigned.',
  'I authorize the employees, partners or Coaches of Starsportsus LLC / Major League Cricket Academy Philadelphia to call for Emergency Services for myself or my child/children. I authorize attending physician at hospital to administer necessary emergency medical care and I accept the responsibility for payment of any and all treatment provided therein including emergency rescue services.',
  'I hereby waive, release, indemnify, absolve and hold harmless Starsportsus LLC / Major League Cricket Academy Philadelphia, the organizers, sponsors, representatives, partners, coaches, and other participants, for any claim arising out of injury or accidental death to me / my child.',
  'I assume all risks, liabilities and hazards accidental to participating in an instructional clinic, practice sessions, bowling / pitching machine practice and / or party / function at Starsportsus LLC / Major League Cricket Academy Philadelphia.',
  'I hereby acknowledge and agree that participation in any activity at StarSportsUS / Major League Cricket Academy Philadelphia comes with inherent risks. I have full knowledge and understanding of the inherent risks associated with participation, including but in no way limited to: (1) slips, trips, and falls, (2) athletic injuries, and (3) illness, including exposure to and infection with viruses or bacteria.',
  'I further acknowledge that the preceding list is not inclusive of all possible risks associated with participation and that said list in no way limits the operation of this Agreement.',
  'Coronavirus / COVID-19 Warning & Disclaimer - Coronavirus, COVID-19 and its variant is an extremely contagious virus that spreads easily through person-to-person contact. Federal and state authorities recommend social distancing as a mean to prevent the spread of the virus. COVID-19 / its variant can lead to severe illness, personal injury, permanent disability, and death. Participating in any activity at StarSportsUS / Major League Cricket Academy Philadelphia or accessing StarSportsUS / Major League Cricket Academy Philadelphia facilities could increase the risk of contracting COVID-19 / its variant. StarSportsUS / Major League Cricket Academy Philadelphia in no way warrants that COVID-19 / COVID-19 Variant infection will not occur through participation in any activity at StarSportsUS / Major League Cricket Academy Philadelphia.',
  'I further certify that I am in good health and that I have no conditions or impairments which would preclude my safe participation in any activity at StarSportsUS / Major League Cricket Academy Philadelphia or service. I hereby certify that I have read the StarSportsUS / Major League Cricket Academy Philadelphia policies and procedures related to the transmission of COVID-19 / its variant and agree to follow these as written.',
  'I will produce proof of Vaccination if asked by any StarSportsUS or Major League Academy Philadelphia officials.'
];

function isValidShopifyWebhook(req) {
  const secret = process.env.SHOPIFY_WEBHOOK_SECRET;
  const hmac = req.headers['x-shopify-hmac-sha256'];

  if (!secret || !hmac || !Buffer.isBuffer(req.body)) return false;

  const digest = crypto
    .createHmac('sha256', secret)
    .update(req.body)
    .digest('base64');

  const digestBuffer = Buffer.from(digest);
  const hmacBuffer = Buffer.from(String(hmac));
  return digestBuffer.length === hmacBuffer.length && crypto.timingSafeEqual(digestBuffer, hmacBuffer);
}

const getOrderLineProperty = (lineItem = {}, key) => {
  const searchKey = key.trim().toLowerCase();
  const properties = lineItem.properties || [];

  if (Array.isArray(properties)) {
    const match = properties.find(prop =>
      String(prop.name || prop.key || '').trim().toLowerCase() === searchKey
    );
    return match?.value || '';
  }

  if (properties && typeof properties === 'object') {
    const matchKey = Object.keys(properties).find(propKey =>
      propKey.trim().toLowerCase() === searchKey
    );
    return matchKey ? properties[matchKey] : '';
  }

  return '';
};

const getPaidOrderCustomerEmail = (order = {}) =>
  order.email || order.customer?.email || order.contact_email || '';

async function savePendingCoachingWaiver(waiver, pdfBuffer) {
  await pool.query(createCoachingWaiversTableSQL);

  const waiverId = crypto.randomUUID();
  const childNames = (waiver.children || [])
    .map(child => `${child.first_name || ''} ${child.last_name || ''}`.trim())
    .filter(Boolean);
  const payloadForStorage = { ...waiver };
  delete payloadForStorage.signature_data_url;

  await pool.query(
    `
      INSERT INTO coaching_waivers (
        waiver_id,
        customer_email,
        parent_first_name,
        parent_last_name,
        participant_name,
        emergency_contact_name,
        emergency_contact_phone,
        client_ip,
        status,
        waiver_payload,
        pdf,
        submitted_at
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'pending_checkout', $9::jsonb, $10, $11)
    `,
    [
      waiverId,
      waiver.parent_email,
      waiver.parent_first_name,
      waiver.parent_last_name,
      childNames.join(', '),
      waiver.emergency_contact_name,
      waiver.emergency_contact_phone,
      waiver.client_ip,
      JSON.stringify(payloadForStorage),
      pdfBuffer,
      waiver.submitted_at
    ]
  );

  return waiverId;
}

async function markCoachingWaiverEmailed(waiverId) {
  await pool.query(
    'UPDATE coaching_waivers SET emailed_at = NOW(), updated_at = NOW() WHERE waiver_id = $1',
    [waiverId]
  );
}

async function findSubscriptionIdForOrderLine(order, lineItem) {
  const email = getPaidOrderCustomerEmail(order);
  const childFirstName = getOrderLineProperty(lineItem, 'Child First Name');
  const childLastName = getOrderLineProperty(lineItem, 'Child Last Name');
  const childDob = getOrderLineProperty(lineItem, 'Child DOB');

  if (!email || !childFirstName || !childLastName) return '';

  try {
    const searchRes = await fetch(`https://app.sealsubscriptions.com/shopify/merchant/api/subscriptions?query=${encodeURIComponent(email)}`, {
      headers: { 'X-Seal-Token': SEAL_TOKEN, 'Content-Type': 'application/json' },
    });

    if (!searchRes.ok) return '';

    const searchData = await searchRes.json();
    const subscriptions = searchData.payload?.subscriptions || [];

    for (const sub of subscriptions) {
      const detail = await fetchSealSubscriptionDetail(sub.id);
      const items = detail.items || detail.payload?.items || [];

      for (const item of items) {
        const props = item.properties || item.product_properties || [];
        const firstName = getItemProperty(props, 'Child First Name');
        const lastName = getItemProperty(props, 'Child Last Name');
        const dob = getItemProperty(props, 'Child DOB');

        if (
          normalizeSearchText(firstName) === normalizeSearchText(childFirstName) &&
          normalizeSearchText(lastName) === normalizeSearchText(childLastName) &&
          (!childDob || !dob || String(dob).slice(0, 10) === String(childDob).slice(0, 10))
        ) {
          return String(sub.id);
        }
      }
    }
  } catch (err) {
    console.error('Unable to match waiver to Seal subscription:', err);
  }

  return '';
}

async function finalizeCoachingWaiversForPaidOrder(order) {
  const financialStatus = String(order.financial_status || '').toLowerCase();
  const paymentStatus = String(order.payment_status || '').toLowerCase();
  const isPaid = ['paid', 'partially_paid'].includes(financialStatus) || ['paid', 'partially_paid'].includes(paymentStatus);

  if (!isPaid) {
    return {
      skipped: true,
      reason: `Order is not paid yet. financial_status=${financialStatus || 'unknown'}`
    };
  }

  const lineItems = order.line_items || [];
  const matchedWaiverIds = [];

  for (const lineItem of lineItems) {
    const waiverId = getOrderLineProperty(lineItem, 'Waiver ID') || getOrderLineProperty(lineItem, '_Waiver ID');
    if (!waiverId) continue;

    const subscriptionId = await findSubscriptionIdForOrderLine(order, lineItem);

    await pool.query(
      `
        UPDATE coaching_waivers
        SET
          subscription_id = COALESCE(NULLIF($2, ''), subscription_id),
          order_id = $3,
          order_name = $4,
          status = 'paid',
          paid_at = NOW(),
          updated_at = NOW()
        WHERE waiver_id = $1
      `,
      [
        waiverId,
        subscriptionId,
        String(order.id || ''),
        String(order.name || order.order_number || '')
      ]
    );

    matchedWaiverIds.push(waiverId);
  }

  return { matched_waiver_ids: matchedWaiverIds };
}

async function finalizeTrialSessionsForPaidOrder(order) {
  const financialStatus = String(order.financial_status || '').toLowerCase();
  const paymentStatus = String(order.payment_status || '').toLowerCase();
  const isPaid = ['paid', 'partially_paid'].includes(financialStatus) || ['paid', 'partially_paid'].includes(paymentStatus);

  if (!isPaid) {
    return { skipped: true, reason: `Order is not paid yet. financial_status=${financialStatus || 'unknown'}` };
  }

  const lineItems = order.line_items || [];
  const createdTrialIds = [];
  const convertedTrialIds = [];

  for (const lineItem of lineItems) {
    const trialCheckoutId = getOrderLineProperty(lineItem, 'Trial Checkout ID');
    if (trialCheckoutId) {
      await pool.query(
        `
          INSERT INTO trial_sessions (
            trial_session_id,
            status,
            order_id,
            order_name,
            customer_email,
            parent_first_name,
            parent_last_name,
            parent_mobile,
            referred_by,
            participant_first_name,
            participant_last_name,
            participant_dob,
            location_id,
            batch_id,
            trial_date,
            trial_time,
            trial_end_time,
            paid_at
          )
          VALUES ($1, 'paid', $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::date, $12, $13, $14::date, $15, $16, NOW())
          ON CONFLICT (trial_session_id) DO UPDATE
          SET
            status = 'paid',
            order_id = EXCLUDED.order_id,
            order_name = EXCLUDED.order_name,
            paid_at = COALESCE(trial_sessions.paid_at, NOW()),
            updated_at = NOW()
        `,
        [
          trialCheckoutId,
          String(order.id || ''),
          String(order.name || order.order_number || ''),
          getPaidOrderCustomerEmail(order) || getOrderLineProperty(lineItem, 'Parent Email'),
          getOrderLineProperty(lineItem, 'Parent First Name'),
          getOrderLineProperty(lineItem, 'Parent Last Name'),
          getOrderLineProperty(lineItem, 'Parent Mobile'),
          getOrderLineProperty(lineItem, 'Referred By'),
          getOrderLineProperty(lineItem, 'Participant First Name'),
          getOrderLineProperty(lineItem, 'Participant Last Name'),
          getOrderLineProperty(lineItem, 'Participant DOB'),
          Number(getOrderLineProperty(lineItem, 'Trial Location ID') || 0) || null,
          Number(getOrderLineProperty(lineItem, 'Trial Batch ID') || 0) || null,
          getOrderLineProperty(lineItem, 'Trial Date'),
          getOrderLineProperty(lineItem, 'Trial Time'),
          getOrderLineProperty(lineItem, 'Trial End Time') || null
        ]
      );
      createdTrialIds.push(trialCheckoutId);
    }

    const trialSessionId = getOrderLineProperty(lineItem, 'Trial Session ID');
    if (trialSessionId) {
      const subscriptionId = await findSubscriptionIdForOrderLine(order, lineItem);
      const day1BatchId = Number(getOrderLineProperty(lineItem, 'Day 1 Batch ID') || 0);
      const day2BatchId = Number(getOrderLineProperty(lineItem, 'Day 2 Batch ID') || 0);
      const batchIds = [day1BatchId, day2BatchId].filter(id => Number.isInteger(id) && id > 0);

      await pool.query(
        `
          UPDATE trial_sessions
          SET converted_to_coaching = true,
              converted_at = NOW(),
              updated_at = NOW()
          WHERE trial_session_id = $1
        `,
        [trialSessionId]
      );

      if (subscriptionId && batchIds.length) {
        await replaceBatchAssignmentsForSubscription({
          subscriptionId,
          batchIds,
          effectiveFromDate: getDateStringInTimeZone(new Date()),
          closeExisting: true
        });
      }

      convertedTrialIds.push(trialSessionId);
    }
  }

  return { created_trial_ids: createdTrialIds, converted_trial_ids: convertedTrialIds };
}

const buildCoachingWaiverPdf = (waiver) => new Promise((resolve, reject) => {
  const doc = new PDFDocument({ margin: 50, size: 'LETTER' });
  const chunks = [];

  doc.on('data', chunk => chunks.push(chunk));
  doc.on('error', reject);
  doc.on('end', () => resolve(Buffer.concat(chunks)));

  const children = Array.isArray(waiver.children) ? waiver.children : [];
  const submittedAt = waiver.submitted_at || new Date().toISOString();

  doc.fontSize(18).text('Liability Waiver', { align: 'center' });
  doc.moveDown();
  doc.fontSize(10).text(`Submitted: ${new Date(submittedAt).toLocaleString('en-US', { timeZone: 'America/New_York' })} ET`);
  doc.text(`Client IP Address: ${waiver.client_ip || ''}`);
  doc.moveDown();

  doc.fontSize(13).text('Guardian Information', { underline: true });
  doc.fontSize(10)
    .text(`Name: ${waiver.parent_first_name || ''} ${waiver.parent_last_name || ''}`)
    .text(`Email: ${waiver.parent_email || ''}`)
    .text(`Mobile: ${waiver.parent_mobile || ''}`)
    .text(`Address: ${[waiver.parent_address1, waiver.parent_address2, waiver.parent_city, waiver.parent_state, waiver.parent_zip].filter(Boolean).join(', ')}`)
    .text(`Emergency Contact: ${waiver.emergency_contact_name || ''} ${waiver.emergency_contact_phone ? `(${waiver.emergency_contact_phone})` : ''}`);
  doc.moveDown();

  doc.fontSize(13).text('Participant Information', { underline: true });
  children.forEach((child, index) => {
    doc.fontSize(10).text(`${index + 1}. ${child.first_name || ''} ${child.last_name || ''} | DOB: ${child.dob || ''} | Program: ${child.program || ''} | Billing: ${child.billing_interval || ''}`);
  });
  doc.moveDown();

  doc.fontSize(13).text('Waiver Agreement', { underline: true });
  doc.fontSize(10).text(COACHING_WAIVER_TITLE, { align: 'left' });
  doc.moveDown(0.5);
  COACHING_WAIVER_ITEMS.forEach((item, index) => {
    doc.text(`${index + 1}. ${item}`, { align: 'left' });
    doc.moveDown(0.35);
  });
  doc.moveDown();

  doc.fontSize(13).text('Signature', { underline: true });
  doc.fontSize(10).text(`Signed Name: ${waiver.signature_name || ''}`);
  doc.text(`Signed At: ${new Date(submittedAt).toLocaleString('en-US', { timeZone: 'America/New_York' })} ET`);
  doc.moveDown(0.5);

  if (waiver.signature_data_url) {
    const signatureBuffer = Buffer.from(String(waiver.signature_data_url).replace(/^data:image\/png;base64,/, ''), 'base64');
    doc.image(signatureBuffer, { fit: [250, 100] });
  }

  doc.end();
});

async function sendCoachingWaiverEmail(toEmail, pdfBuffer, waiver) {
  const childNames = (waiver.children || [])
    .map(child => `${child.first_name || ''} ${child.last_name || ''}`.trim())
    .filter(Boolean)
    .join(', ');

  const html = `
    <p>Hi ${escapeHtml(waiver.parent_first_name || '')},</p>
    <p>Attached is a copy of your signed coaching waiver${childNames ? ` for ${escapeHtml(childNames)}` : ''}.</p>
    <p>Client IP Address captured at signing: ${escapeHtml(waiver.client_ip || '')}</p>
    <p>Thank you,<br>MLCA Coaching</p>
  `;

  await sgMail.send({
    to: toEmail,
    from: process.env.EMAIL_FROM,
    subject: 'Signed Coaching Waiver',
    html,
    attachments: [{
      content: pdfBuffer.toString('base64'),
      filename: 'mlca-coaching-waiver.pdf',
      type: 'application/pdf',
      disposition: 'attachment'
    }]
  });
}

app.post('/coaching-waiver', async (req, res) => {
  const waiver = {
    ...req.body,
    client_ip: getClientIpAddress(req),
    submitted_at: new Date().toISOString()
  };

  if (!waiver.parent_first_name || !waiver.parent_last_name || !waiver.parent_email || !waiver.parent_mobile) {
    return res.status(400).json({ success: false, error: 'Missing required parent information.' });
  }

  if (!waiver.emergency_contact_name || !waiver.emergency_contact_phone) {
    return res.status(400).json({ success: false, error: 'Missing emergency contact information.' });
  }

  if (!Array.isArray(waiver.children) || waiver.children.length === 0) {
    return res.status(400).json({ success: false, error: 'Missing participant information.' });
  }

  if (!waiver.signature_name || !/^data:image\/png;base64,/.test(String(waiver.signature_data_url || ''))) {
    return res.status(400).json({ success: false, error: 'Missing signature.' });
  }

  let pdfBuffer;
  let waiverId;

  try {
    pdfBuffer = await buildCoachingWaiverPdf(waiver);
  } catch (err) {
    console.error('Error creating coaching waiver PDF:', err);
    return res.status(500).json({ success: false, error: 'Unable to create waiver PDF. Please try again.' });
  }

  try {
    waiverId = await savePendingCoachingWaiver(waiver, pdfBuffer);
  } catch (err) {
    console.error('Error saving coaching waiver:', err);
    return res.status(500).json({ success: false, error: 'Unable to save waiver. Please try again.' });
  }

  return res.json({ success: true, waiver_id: waiverId });
});

app.get('/admin/coaching-waivers', async (req, res) => {
  if (!requireAdminKey(req, res)) return;

  const limit = Math.min(Math.max(parseInt(req.query.limit || '25', 10) || 25, 1), 100);

  try {
    const { rows } = await pool.query(
      `
        SELECT
          waiver_id,
          subscription_id,
          order_id,
          order_name,
          customer_email,
          parent_first_name,
          parent_last_name,
          participant_name,
          emergency_contact_name,
          emergency_contact_phone,
          client_ip,
          status,
          submitted_at,
          emailed_at,
          paid_at,
          created_at,
          OCTET_LENGTH(pdf) AS pdf_size
        FROM coaching_waivers
        ORDER BY created_at DESC
        LIMIT $1
      `,
      [limit]
    );

    return res.json({ success: true, waivers: rows });
  } catch (err) {
    console.error('Error listing coaching waivers:', err);
    return res.status(500).json({ success: false, error: 'Unable to list coaching waivers.' });
  }
});

app.get('/admin/coaching-waivers/:waiverId/pdf', async (req, res) => {
  if (!requireAdminKey(req, res)) return;

  try {
    const { rows } = await pool.query(
      `
        SELECT waiver_id, pdf
        FROM coaching_waivers
        WHERE waiver_id = $1
        LIMIT 1
      `,
      [req.params.waiverId]
    );

    if (!rows.length) {
      return res.status(404).json({ success: false, error: 'Waiver not found.' });
    }

    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `attachment; filename=coaching-waiver-${rows[0].waiver_id}.pdf`);
    return res.send(rows[0].pdf);
  } catch (err) {
    console.error('Error downloading coaching waiver PDF:', err);
    return res.status(500).json({ success: false, error: 'Unable to download coaching waiver PDF.' });
  }
});

app.get('/admin/coaching-waivers-pending-email', async (req, res) => {
  if (!requireAdminKey(req, res)) return;

  const limit = Math.min(Math.max(parseInt(req.query.limit || '25', 10) || 25, 1), 100);

  try {
    const { rows } = await pool.query(
      `
        SELECT
          waiver_id,
          subscription_id,
          order_id,
          order_name,
          customer_email,
          parent_first_name,
          parent_last_name,
          participant_name,
          emergency_contact_name,
          emergency_contact_phone,
          client_ip,
          status,
          waiver_payload,
          submitted_at,
          paid_at,
          created_at,
          OCTET_LENGTH(pdf) AS pdf_size
        FROM coaching_waivers
        WHERE status = 'paid'
          AND emailed_at IS NULL
        ORDER BY paid_at ASC NULLS LAST, created_at ASC
        LIMIT $1
      `,
      [limit]
    );

    return res.json({ success: true, waivers: rows });
  } catch (err) {
    console.error('Error listing coaching waivers pending email:', err);
    return res.status(500).json({ success: false, error: 'Unable to list waivers pending email.' });
  }
});

app.post('/admin/coaching-waivers/:waiverId/mark-emailed', async (req, res) => {
  if (!requireAdminKey(req, res)) return;

  try {
    const { rows } = await pool.query(
      `
        UPDATE coaching_waivers
        SET emailed_at = NOW(), updated_at = NOW()
        WHERE waiver_id = $1
        RETURNING waiver_id, customer_email, status, emailed_at
      `,
      [req.params.waiverId]
    );

    if (!rows.length) {
      return res.status(404).json({ success: false, error: 'Waiver not found.' });
    }

    return res.json({ success: true, waiver: rows[0] });
  } catch (err) {
    console.error('Error marking coaching waiver emailed:', err);
    return res.status(500).json({ success: false, error: 'Unable to mark waiver emailed.' });
  }
});

app.post('/admin/coaching-waivers/:waiverId/test-payment', async (req, res) => {
  if (!requireAdminKey(req, res)) return;

  const waiverId = req.params.waiverId;
  const subscriptionId = String(req.body?.subscription_id || req.query.subscription_id || `TEST-SUB-${Date.now()}`);
  const orderId = String(req.body?.order_id || req.query.order_id || `TEST-ORDER-${Date.now()}`);
  const orderName = String(req.body?.order_name || req.query.order_name || `#TEST-${Date.now()}`);

  try {
    const { rows } = await pool.query(
      `
        UPDATE coaching_waivers
        SET
          subscription_id = $2,
          order_id = $3,
          order_name = $4,
          status = 'paid',
          paid_at = NOW(),
          updated_at = NOW()
        WHERE waiver_id = $1
        RETURNING
          waiver_id,
          subscription_id,
          order_id,
          order_name,
          customer_email,
          participant_name,
          status,
          paid_at,
          OCTET_LENGTH(pdf) AS pdf_size
      `,
      [waiverId, subscriptionId, orderId, orderName]
    );

    if (!rows.length) {
      return res.status(404).json({ success: false, error: 'Waiver not found.' });
    }

    return res.json({ success: true, waiver: rows[0] });
  } catch (err) {
    console.error('Error applying test coaching waiver payment:', err);
    return res.status(500).json({ success: false, error: 'Unable to apply test payment.' });
  }
});

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
