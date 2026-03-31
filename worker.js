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

function envOptional(name, fallback) {
  const v = process.env[name];
  if (v === undefined || v === null) return fallback;
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

function normalizeAgencyStatus(v) {
  return normalizeText(v).toLowerCase();
}

function isDeletedAgencyStatus(v) {
  const status = normalizeAgencyStatus(v);
  return status === "quit" || status === "left_agency";
}

function isInAgencyStatus(v) {
  return normalizeAgencyStatus(v) === "in_agency";
}

function clamp255(v) {
  const txt = normalizeText(v);
  if (!txt) return "";
  return txt.length <= 255 ? txt : txt.slice(0, 255);
}

function sleepMs(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function uniq(arr) {
  const out = [];
  const seen = new Set();
  for (const item of arr) {
    const v = s(item);
    if (!v) continue;
    if (seen.has(v)) continue;
    seen.add(v);
    out.push(v);
  }
  return out;
}

function chunk10(arr) {
  const out = [];
  for (let i = 0; i < arr.length; i += 10) out.push(arr.slice(i, i + 10));
  return out;
}

function extractInsideParens(v) {
  const txt = s(v);
  if (!txt) return "";
  const open = txt.indexOf("(");
  const close = txt.lastIndexOf(")");
  if (open >= 0 && close > open) {
    const inside = txt.slice(open + 1, close).trim();
    return inside || txt;
  }
  return txt;
}

function emailLocalPart(v) {
  const txt = s(v);
  if (!txt) return "";
  const at = txt.indexOf("@");
  if (at > 0) {
    const left = txt.slice(0, at).trim();
    return left || txt;
  }
  return txt;
}

function formatNumber(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return "0";
  return new Intl.NumberFormat("en-GB").format(Math.trunc(n));
}

function hoursDecimalToHhMm(v) {
  const n = Number(v);
  if (!Number.isFinite(n) || n <= 0) return "0h 0m";
  const totalMinutes = Math.round(n * 60);
  const h = Math.floor(totalMinutes / 60);
  const m = totalMinutes % 60;
  return String(h) + "h " + String(m) + "m";
}

function ordinalSuffix(day) {
  const d = Number(day);
  if (!Number.isFinite(d)) return "th";
  const mod100 = d % 100;
  if (mod100 >= 11 && mod100 <= 13) return "th";
  const mod10 = d % 10;
  if (mod10 === 1) return "st";
  if (mod10 === 2) return "nd";
  if (mod10 === 3) return "rd";
  return "th";
}

function toDayMonth(v) {
  if (!v) return "";

  let d;
  if (v instanceof Date) {
    d = v;
  } else {
    const txt = typeof v === "string" ? v.trim() : "";
    if (!txt) return "";
    d = new Date(txt);
  }

  if (Number.isNaN(d.getTime())) return "";

  const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  const day = d.getUTCDate();
  const month = months[d.getUTCMonth()] || "";
  if (!month) return "";
  return String(day) + ordinalSuffix(day) + " " + month;
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
    body: body === undefined ? undefined : JSON.stringify(body)
  });
  const text = await res.text();
  return { ok: res.ok, status: res.status, text };
}

async function withQueueRetry(fn, context) {
  const maxAttempts = Number(envOptional("RESPOND_IO_RETRY_MAX", "12"));
  const baseDelayMs = Number(envOptional("RESPOND_IO_RETRY_BASE_MS", "2000"));
  const maxDelayMs = Number(envOptional("RESPOND_IO_RETRY_MAX_MS", "30000"));

  let attempt = 0;

  while (true) {
    attempt += 1;

    const result = await fn();

    if (result.ok) {
      return result;
    }

    const isQueued =
      result.status === 449 &&
      normalizeText(result.text).toLowerCase().includes("in the queue");

    if (!isQueued) {
      return result;
    }

    if (attempt >= maxAttempts) {
      return result;
    }

    const delayMs = Math.min(maxDelayMs, baseDelayMs * Math.pow(2, attempt - 1));
    log("HTTP QUEUE RETRY", context, "attempt=" + attempt + "/" + maxAttempts, "delay_ms=" + delayMs);
    await sleepMs(delayMs);
  }
}

function urlWithPhone(template, phoneE164) {
  return template.replace("{identifier}", "phone:" + phoneE164);
}

async function respondCreateOrUpdate(token, phoneE164, firstName, profilePic, customFields) {
  const base = envRequired("RESPOND_IO_CREATE_OR_UPDATE_URL");
  const url = urlWithPhone(base, phoneE164);

  const body = {
    firstName,
    phone: phoneE164,
    custom_fields: customFields
  };

  if (s(profilePic)) body.profilePic = s(profilePic);

  return await withQueueRetry(
    () => httpCall("POST", url, token, body),
    "CREATE_OR_UPDATE phone=" + phoneE164
  );
}

async function respondDeleteContact(token, phoneE164) {
  const base = envRequired("RESPOND_IO_DELETE_CONTACT_URL");
  const url = urlWithPhone(base, phoneE164);
  return await withQueueRetry(
    () => httpCall("DELETE", url, token, undefined),
    "DELETE_CONTACT phone=" + phoneE164
  );
}

async function respondDeleteTags(token, phoneE164, tags) {
  const base = envRequired("RESPOND_IO_DELETE_TAGS_URL");
  const url = urlWithPhone(base, phoneE164);

  const payload = uniq(tags);
  if (payload.length === 0) return { ok: true, status: 200, text: "" };

  for (const part of chunk10(payload)) {
    const result = await withQueueRetry(
      () => httpCall("DELETE", url, token, part),
      "DELETE_TAGS phone=" + phoneE164 + " tags=" + JSON.stringify(part)
    );

    if (!result.ok) {
      log("DELETE TAGS FAILED", phoneE164, JSON.stringify(part), result.status, result.text);
      return result;
    }
  }

  return { ok: true, status: 200, text: "" };
}

async function respondAddTags(token, phoneE164, tags) {
  const base = envRequired("RESPOND_IO_ADD_TAGS_URL");
  const url = urlWithPhone(base, phoneE164);

  const payload = uniq(tags);
  if (payload.length === 0) return { ok: true, status: 200, text: "" };

  for (const part of chunk10(payload)) {
    const result = await withQueueRetry(
      () => httpCall("POST", url, token, part),
      "ADD_TAGS phone=" + phoneE164 + " tags=" + JSON.stringify(part)
    );

    if (!result.ok) {
      log("ADD TAG FAILED", phoneE164, JSON.stringify(part), result.status, result.text);
      return result;
    }
  }

  return { ok: true, status: 200, text: "" };
}

async function respondUpdateLifecycle(token, phoneE164, lifecycleName) {
  const base = envRequired("RESPOND_IO_UPDATE_LIFECYCLE_URL");
  const url = urlWithPhone(base, phoneE164);

  const body = { name: s(lifecycleName) ? s(lifecycleName) : "" };

  return await withQueueRetry(
    () => httpCall("POST", url, token, body),
    "UPDATE_LIFECYCLE phone=" + phoneE164 + " lifecycle=" + lifecycleName
  );
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
        u.mobile AS mobile,
        v.phone_e164,
        v.tiktok_username,
        v.real_first_name,
        v.agency_status,
        v.role_tag,
        v.group_raw,
        v.manager_raw,
        v.tier_tag,
        v.profile_pic_url,
        v.stats_as_of,
        v.diamonds_mtd,
        v.valid_days_mtd,
        v.live_duration_mtd_hours,
        v.lifecycle,
        v.yesterdays_diamonds_num,
        v.yesterdays_duration_hours_num,
        v.yesterday_valid_day_bool,
        v.activity_status,
        v.fasttrack_tier,
        v.moving_to,
        v.last_month_diamonds,
        v.battle_pending_summary,
        v.battle_next_summary,
        v.traffic_boost_summary,
        v.incentive_summary
      FROM public.v_respond_sync_users_plus_yesterday_plus_leagues v
      LEFT JOIN public.users u
        ON u.id = v.user_id
      ORDER BY v.user_id
      LIMIT $1
    `, [limit]);

    return res.rows;
  } finally {
    client.release();
  }
}

function dedupeByPhone(rows) {
  const byPhone = new Map();

  for (const r of rows) {
    const phone = normalizePhoneE164(r.mobile || r.phone_e164);
    if (!phone) continue;

    const current = byPhone.get(phone);
    if (!current) byPhone.set(phone, { phone, rows: [r] });
    else current.rows.push(r);
  }

  const out = [];

  for (const entry of byPhone.values()) {
    const rowsSortedDesc = entry.rows.slice().sort((a, b) => Number(b.user_id) - Number(a.user_id));
    const latest = rowsSortedDesc[0];

    if (isDeletedAgencyStatus(latest.agency_status)) {
      out.push({ action: "delete", row: latest, phone: entry.phone });
      continue;
    }

    const inAgencyRows = rowsSortedDesc.filter((x) => isInAgencyStatus(x.agency_status));

    if (inAgencyRows.length > 0) {
      out.push({ action: "sync", row: inAgencyRows[0], phone: entry.phone });
      continue;
    }

    out.push({ action: "delete", row: latest, phone: entry.phone });
  }

  out.sort((a, b) => Number(a.row.user_id) - Number(b.row.user_id));
  return out;
}

async function runSyncOnce() {
  const token = envRequired("RESPOND_IO_TOKEN");
  const limit = Number(envOptional("SYNC_LIMIT", "100000"));
  const paceMs = Number(envOptional("RESPOND_IO_PER_CONTACT_PACE_MS", "500"));

  const rows = await fetchRows(limit);
  const work = dedupeByPhone(rows);

  let ok = 0;
  let fail = 0;

  for (const item of work) {
    const r = item.row;
    const userId = r.user_id;
    const phone = item.phone;
    const username = normalizeText(r.tiktok_username);

    try {
      if (item.action === "delete") {
        const del = await respondDeleteContact(token, phone);
        const treatMissingOk = del.status === 400 || del.status === 404;

        if (!del.ok && !treatMissingOk) {
          throw new Error("Delete contact failed HTTP " + del.status + " " + del.text);
        }

        ok += 1;
        log("OK DELETE", "phone=" + phone, "username=" + username, "agency_status=" + normalizeText(r.agency_status));
        if (paceMs > 0) await sleepMs(paceMs);
        continue;
      }

      const tiktok = normalizeText(r.tiktok_username);
      const realFirst = normalizeText(r.real_first_name);
      const tierTag = normalizeText(r.tier_tag) || "Tier 1";
      const lifecycle = normalizeText(r.lifecycle);
      const fasttrackTier = normalizeText(r.fasttrack_tier);
      const movingTo = normalizeText(r.moving_to);
      const activityStatus = normalizeText(r.activity_status);

      const groupValue = extractInsideParens(normalizeText(r.group_raw));
      const managerValue = emailLocalPart(normalizeText(r.manager_raw));

      const diamondsMtd = formatNumber(r.diamonds_mtd);
      const validDaysMtd = formatNumber(r.valid_days_mtd);
      const liveDurationMtd = hoursDecimalToHhMm(r.live_duration_mtd_hours);
      const statsAsOf = toDayMonth(r.stats_as_of);

      const yDiamonds = formatNumber(r.yesterdays_diamonds_num);
      const yDurationHours = Number(r.yesterdays_duration_hours_num);
      const yDuration = hoursDecimalToHhMm(r.yesterdays_duration_hours_num);
      const yValidDay = Number.isFinite(yDurationHours) && yDurationHours >= 1 ? "Yes" : "No";

      const lastMonthDiamonds = formatNumber(r.last_month_diamonds);

      const battlePendingSummary = clamp255(r.battle_pending_summary);
      const battleNextSummary = clamp255(r.battle_next_summary);
      const trafficBoostSummary = clamp255(r.traffic_boost_summary);
      const incentiveSummary = clamp255(r.incentive_summary);

      const firstName = tiktok ? tiktok : "user_" + String(userId);

      const customFields = [
        { name: "tiktok_username", value: tiktok || null },
        { name: "real_first_name", value: realFirst || null },
        { name: "group", value: groupValue || null },
        { name: "manager", value: managerValue || null },
        { name: "tier", value: tierTag || null },
        { name: "diamonds_mtd", value: clamp255(diamondsMtd) },
        { name: "last_month_diamonds", value: clamp255(lastMonthDiamonds) },
        { name: "valid_days_mtd", value: clamp255(validDaysMtd) },
        { name: "live_duration_mtd", value: clamp255(liveDurationMtd) },
        { name: "yesterdays_diamonds", value: clamp255(yDiamonds) },
        { name: "yesterdays_duration", value: clamp255(yDuration) },
        { name: "yesterday_valid_day", value: yValidDay },
        { name: "fasttrack_tier", value: fasttrackTier || null },
        { name: "moving_to", value: movingTo || null },
        { name: "stats_as_of", value: statsAsOf ? clamp255(statsAsOf) : null },
        { name: "agency_status", value: normalizeText(r.agency_status) || null },
        { name: "battle_pending_summary", value: battlePendingSummary || null },
        { name: "battle_next_summary", value: battleNextSummary || null },
        { name: "traffic_boost_summary", value: trafficBoostSummary || null },
        { name: "incentive_summary", value: incentiveSummary || null }
      ];

      const cu = await respondCreateOrUpdate(token, phone, firstName, s(r.profile_pic_url), customFields);
      if (!cu.ok) {
        throw new Error("Create or update failed HTTP " + cu.status + " " + cu.text);
      }

      const lc = await respondUpdateLifecycle(token, phone, lifecycle);
      if (!lc.ok) {
        throw new Error("Update lifecycle failed HTTP " + lc.status + " " + lc.text);
      }

      const deleteActivity = await respondDeleteTags(token, phone, ["Active", "Cooling", "Dormant"]);
      if (!deleteActivity.ok) {
        throw new Error("Delete activity tags failed HTTP " + deleteActivity.status + " " + deleteActivity.text);
      }

      if (activityStatus) {
        const addActivity = await respondAddTags(token, phone, [activityStatus]);
        if (!addActivity.ok) {
          throw new Error("Add activity tag failed HTTP " + addActivity.status + " " + addActivity.text);
        }
      }

      ok += 1;
      log(
        "OK SYNC",
        "phone=" + phone,
        "username=" + username,
        "lifecycle=" + lifecycle,
        "activity_status=" + activityStatus
      );
    } catch (e) {
      fail += 1;
      log("FAIL", "user_id=" + userId, "phone=" + phone, "username=" + username, "err=" + String(e && e.message ? e.message : e));
    }

    if (paceMs > 0) {
      await sleepMs(paceMs);
    }
  }

  log("SYNC COMPLETE", "ok=" + ok, "fail=" + fail, "total=" + work.length);
}

const app = express();

app.post("/run", async (req, res) => {
  log("RUN TRIGGERED");
  runSyncOnce().catch((err) => {
    log("RUN FATAL", String(err && err.message ? err.message : err));
  });
  res.json({ status: "started" });
});

const port = Number(process.env.PORT || "8080");

app.listen(port, () => {
  log("LISTENING ON PORT", port);
});
