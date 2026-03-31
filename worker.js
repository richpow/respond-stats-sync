import express from "express";
import pg from "pg";

const { Pool } = pg;

function nowIso() {
  return new Date().toISOString();
}

function log(...args) {
  console.log(nowIso(), ...args);
}

function envRequired(name) {
  const v = process.env[name];
  if (!v) throw new Error("Missing env var: " + name);
  return String(v);
}

function s(v) {
  return typeof v === "string" ? v.trim() : "";
}

function normalizeText(v) {
  const txt = s(v);
  if (!txt) return "";
  if (txt.toUpperCase() === "N/A") return "";
  return txt;
}

function chunk10(arr) {
  const out = [];
  for (let i = 0; i < arr.length; i += 10) out.push(arr.slice(i, i + 10));
  return out;
}

function normalizePhoneE164(v) {
  const raw = s(v);
  if (!raw) return "";
  const digits = raw.replace(/\D/g, "");
  if (!digits) return "";
  return "+" + digits;
}

function respondHeaders(token) {
  return {
    Accept: "application/json",
    Authorization: "Bearer " + token,
    "Content-Type": "application/json"
  };
}

async function httpCall(method, url, token, body) {
  const res = await fetch(url, {
    method,
    headers: respondHeaders(token),
    body: body ? JSON.stringify(body) : undefined
  });
  const text = await res.text();
  return { ok: res.ok, status: res.status, text };
}

function urlWithPhone(template, phoneE164) {
  return template.replace("{identifier}", "phone:" + phoneE164);
}

async function respondCreateOrUpdate(token, phoneE164, firstName, customFields) {
  const base = envRequired("RESPOND_IO_CREATE_OR_UPDATE_URL");
  const url = urlWithPhone(base, phoneE164);

  return await httpCall("POST", url, token, {
    firstName,
    phone: phoneE164,
    custom_fields: customFields
  });
}

async function respondDeleteTags(token, phoneE164, tags) {
  const base = envRequired("RESPOND_IO_DELETE_TAGS_URL");
  const url = urlWithPhone(base, phoneE164);

  for (const part of chunk10(tags)) {
    const r = await httpCall("DELETE", url, token, part);

    if (!r.ok) {
      log("DELETE TAGS FAILED", phoneE164, part, r.status, r.text);
      // DO NOT FAIL
    }
  }

  return { ok: true };
}

async function respondAddTags(token, phoneE164, tags) {
  const base = envRequired("RESPOND_IO_ADD_TAGS_URL");
  const url = urlWithPhone(base, phoneE164);

  for (const part of chunk10(tags)) {
    const r = await httpCall("POST", url, token, part);

    if (!r.ok) {
      log("ADD TAG FAILED", phoneE164, part, r.status, r.text);
      // DO NOT FAIL
    }
  }

  return { ok: true };
}

const pool = new Pool({
  connectionString: envRequired("DATABASE_URL")
});

async function fetchRows(limit) {
  const client = await pool.connect();
  try {
    const res = await client.query(`
      SELECT
        v.user_id,
        u.mobile,
        v.tiktok_username,
        v.real_first_name,
        v.agency_status,
        v.role_tag,
        v.activity_status
      FROM public.v_respond_sync_users_plus_yesterday_plus_leagues v
      LEFT JOIN public.users u ON u.id = v.user_id
      WHERE v.role_tag = 'Creator'
      ORDER BY v.user_id
      LIMIT $1
    `, [limit]);

    return res.rows;
  } finally {
    client.release();
  }
}

async function runSyncOnce() {
  const token = envRequired("RESPOND_IO_TOKEN");
  const rows = await fetchRows(100000);

  for (const r of rows) {
    const phone = normalizePhoneE164(r.mobile);
    if (!phone) continue;

    const firstName = r.tiktok_username || "user_" + r.user_id;

    await respondCreateOrUpdate(token, phone, firstName, []);

    // ===== ACTIVITY TAGS =====
    const activityTags = ["Active", "Cooling", "Dormant"];

    await respondDeleteTags(token, phone, activityTags);

    const status = normalizeText(r.activity_status);

    if (status) {
      await respondAddTags(token, phone, [status]);
    }
  }

  log("SYNC COMPLETE");
}

const app = express();

app.post("/run", async (req, res) => {
  runSyncOnce();
  res.json({ status: "started" });
});

app.listen(process.env.PORT || 8080);
