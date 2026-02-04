// push_rentcafe_snapshot_to_supabase_v6.js
// RentCafe universal runner: DOM extraction -> snapshot upsert -> diff -> unit_events insert
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { chromium } from "playwright";
import fs from "fs";

// ================== ENV ==================
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const RENTCAFE_PROPERTY_ID = process.env.RENTCAFE_PROPERTY_ID;

// Support either a single URL or a comma-separated list
const RENTCAFE_URL = process.env.RENTCAFE_URL;
const RENTCAFE_URLS = process.env.RENTCAFE_URLS;

// Optional toggles
const RENTCAFE_SNIFF_JSON = String(process.env.RENTCAFE_SNIFF_JSON || "").toLowerCase() === "true";

// If true, allow fallback parsing from HTML when DOM units not found (can overcount!)
const RENTCAFE_FLOORPLAN_FALLBACK =
  String(process.env.RENTCAFE_FLOORPLAN_FALLBACK || "").toLowerCase() === "true";

// Source label written into unit_events
const EVENT_SOURCE = process.env.EVENT_SOURCE || "rentcafe";

// Optional override
const SNAPSHOT_DATE = process.env.SNAPSHOT_DATE; // YYYY-MM-DD

// ===== New stability controls =====
const HEADLESS = String(process.env.RENTCAFE_HEADLESS || "true").toLowerCase() !== "false";
const SLOW_MO_MS = Number(process.env.RENTCAFE_SLOWMO_MS || 0);
const GOTO_TIMEOUT_MS = Number(process.env.RENTCAFE_GOTO_TIMEOUT_MS || 90000); // was 60000
const NAV_RETRIES = Number(process.env.RENTCAFE_NAV_RETRIES || 2); // retries after first attempt
const WAIT_UNTIL = String(process.env.RENTCAFE_WAIT_UNTIL || "domcontentloaded"); // domcontentloaded | load | networkidle
const PER_URL_DELAY_MS = Number(process.env.RENTCAFE_PER_URL_DELAY_MS || 500);
const SETTLE_MS = Number(process.env.RENTCAFE_SETTLE_MS || 2500);

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in .env");
  process.exitCode = 1;
  throw new Error("Missing Supabase env");
}
if (!RENTCAFE_PROPERTY_ID) {
  console.error("Missing RENTCAFE_PROPERTY_ID in .env");
  process.exitCode = 1;
  throw new Error("Missing RENTCAFE_PROPERTY_ID");
}
if (!RENTCAFE_URL && !RENTCAFE_URLS) {
  console.error("Missing RENTCAFE_URL or RENTCAFE_URLS in .env");
  process.exitCode = 1;
  throw new Error("Missing RENTCAFE_URL/RENTCAFE_URLS");
}

// ================== SUPABASE ==================
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

// ================== HELPERS ==================
function todayYYYYMMDD() {
  const d = new Date();
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function normalizeUrlList() {
  const urls = [];
  if (RENTCAFE_URLS) {
    for (const u of RENTCAFE_URLS.split(",")) {
      const t = u.trim();
      if (t) urls.push(t);
    }
  } else if (RENTCAFE_URL) {
    urls.push(RENTCAFE_URL.trim());
  }
  return urls;
}

function stripTags(html) {
  return html
    .replace(/<script[\s\S]*?<\/script>/gi, "")
    .replace(/<style[\s\S]*?<\/style>/gi, "")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function firstMatch(text, regex) {
  const m = text.match(regex);
  return m ? m[1] : null;
}

function toNumberMaybe(s) {
  if (s == null) return null;
  const cleaned = String(s).replace(/[^\d.]/g, "");
  if (!cleaned) return null;
  const n = Number(cleaned);
  return Number.isFinite(n) ? n : null;
}

function parseMoneyToNumber(s) {
  return toNumberMaybe(s);
}

function normalizeUnitNumber(x) {
  const n = String(x ?? "").trim();
  return n ? n : null;
}

function normalizeUnitKeyFromNumber(unitNumber) {
  const n = normalizeUnitNumber(unitNumber);
  if (!n) return null;
  return n.startsWith("unit:") ? n : `unit:${n}`;
}

function unitNumberFromUnitKey(unitKey) {
  if (!unitKey) return null;
  const s = String(unitKey);
  if (s.startsWith("unit:")) return s.slice(5) || null;
  return s || null;
}

function diffKeys(prevUnits, currUnits) {
  const prevSet = new Set((prevUnits || []).map((u) => u.unit_key).filter(Boolean));
  const currSet = new Set((currUnits || []).map((u) => u.unit_key).filter(Boolean));

  const appeared = [...currSet].filter((k) => !prevSet.has(k));
  const disappeared = [...prevSet].filter((k) => !currSet.has(k));

  return { appeared, disappeared };
}

function isSecureCafeUrl(url) {
  const u = String(url || "").toLowerCase();
  return u.includes("securecafe.com/onlineleasing") || u.includes("availableunits.aspx");
}

function safeFilename(s) {
  return String(s || "")
    .replace(/^https?:\/\//i, "")
    .replace(/[^\w.-]+/g, "_")
    .slice(0, 180);
}

// ================== SECURECAFE EXTRACTOR (ROBUST) ==================
async function extractAvailableUnitsFromSecureCafe(page, pageUrl) {
  await page.waitForTimeout(1500);

  const rows = await page.evaluate(() => {
    const raw = (document.body?.innerText || "").replace(/\u00a0/g, " ").replace(/\r/g, "");

    const lines = raw
      .split("\n")
      .map((l) => l.replace(/\s+/g, " ").trim())
      .filter(Boolean);

    const out = [];

    const unitLineRegex =
      /^#\s*([A-Za-z0-9-]{1,20})\s+(\d{3,5})\s*\$([0-9,]+)(?:\s*-\s*\$?([0-9,]+))?/;

    const dateRegex = /\b(\d{1,2}\/\d{1,2}\/\d{4})\b/;
    const hasAvailable = /\bAvailable\b/i;

    for (const line of lines) {
      const m = line.match(unitLineRegex);
      if (!m) continue;

      const unit = m[1];
      const sqft = Number(String(m[2]).replace(/,/g, "")) || null;

      const lo = m[3] ? `$${m[3]}` : null;
      const hi = m[4] ? `$${m[4]}` : null;
      const priceText = lo && hi ? `${lo}-${hi}` : lo;

      const d = line.match(dateRegex);
      const available_on = d ? d[1] : hasAvailable.test(line) ? "Available" : null;

      out.push({
        unit_number: unit,
        sqft,
        price_text: priceText,
        available_on,
        raw: line,
      });
    }

    return out.slice(0, 5000);
  });

  const results = [];
  const seenUnitKeys = new Set();

  for (const r of rows) {
    const unit_number = normalizeUnitNumber(r.unit_number);
    const unit_key = unit_number ? normalizeUnitKeyFromNumber(unit_number) : null;
    if (!unit_key) continue;

    if (seenUnitKeys.has(unit_key)) continue;
    seenUnitKeys.add(unit_key);

    results.push({
      unit_key,
      unit_id: unit_key,
      unit_number,
      available_on: r.available_on || null,
      price: r.price_text ? parseMoneyToNumber(r.price_text) : null,
      floor_plan_id: null,
      meta: {
        source: "RentCafe",
        kind: "securecafe_units_global",
        page_url: pageUrl,
        price_text: r.price_text || null,
        sqft: r.sqft ?? null,
        raw: r.raw,
      },
    });
  }

  return results;
}

// ================== DOM-FIRST EXTRACTION (MARKETING SITE) ==================
async function extractAvailableUnitsFromDom(page, pageUrl) {
  await page.waitForTimeout(1000);
  await page.waitForSelector("#availApts", { timeout: 5000 }).catch(() => null);

  const rawBlocks = await page.evaluate(() => {
    const root = document.querySelector("#availApts") || document.body;

    const candidates = Array.from(
      root.querySelectorAll(
        "li, .row, .card, .available, [class*='avail'], [class*='unit'], [id*='avail']"
      )
    );

    const blocks = candidates
      .map((el) => el.innerText || "")
      .map((t) => t.replace(/\s+/g, " ").trim())
      .filter((t) => t.length >= 15)
      .filter((t) => /\$[0-9,]+/.test(t) || /\bAvailable\b/i.test(t));

    const seen = new Set();
    const out = [];
    for (const b of blocks) {
      const key = b.slice(0, 200);
      if (seen.has(key)) continue;
      seen.add(key);
      out.push(b);
      if (out.length >= 250) break;
    }
    return out;
  });

  const results = [];
  const seen = new Set();

  for (const b of rawBlocks) {
    const unitNum =
      firstMatch(b, /Apartment\s*:\s*#?\s*([A-Za-z0-9-]+)/i) ||
      firstMatch(b, /Apartment\s*#?\s*([A-Za-z0-9-]+)/i) ||
      firstMatch(b, /\bUnit\s*#?\s*([A-Za-z0-9-]+)/i) ||
      firstMatch(b, /\b#\s*([A-Za-z0-9-]{1,20})\b/i);

    const normalizedUnitNumber = normalizeUnitNumber(unitNum);

    const priceText =
      firstMatch(b, /Starting\s+at\s*:\s*(\$[0-9,]+)/i) || firstMatch(b, /(\$[0-9,]+)/);

    const availableText =
      firstMatch(
        b,
        /(Available\s+(?:Now|[A-Za-z]{3,9}\s+[0-9]{1,2}(?:st|nd|rd|th)?))/i
      ) || (/\bAvailable\b/i.test(b) ? "Available" : null);

    if (!normalizedUnitNumber && !priceText) continue;

    const unit_key = normalizeUnitKeyFromNumber(normalizedUnitNumber);
    if (!unit_key) continue; // require real unit number

    const key = `${unit_key}|${priceText || ""}|${availableText || ""}`;
    if (seen.has(key)) continue;
    seen.add(key);

    results.push({
      unit_key,
      unit_id: unit_key,
      unit_number: normalizedUnitNumber,
      available_on: availableText || null,
      price: priceText ? parseMoneyToNumber(priceText) : null,
      floor_plan_id: null,
      meta: {
        source: "RentCafe",
        kind: "marketing_dom_units",
        page_url: pageUrl,
        price_text: priceText || null,
        raw: b,
      },
    });
  }

  return results;
}

// ================== FLOORPLAN FALLBACK (OPTIONAL) ==================
function parseFloorplansFromHtml(html, pageUrl) {
  const markers = [
    "fp-card",
    "floorplan-card",
    "floorplan-item",
    "fpWrapper",
    "js-floorplan",
    "floorplanName",
    "Floor Plan",
  ];

  let blocks = [];
  for (const mk of markers) {
    const parts = html.split(mk);
    if (parts.length >= 3) {
      blocks = parts.slice(1).map((p) => mk + p).slice(0, 250);
      break;
    }
  }

  if (blocks.length === 0) {
    const parts = html.split(/Available/i);
    if (parts.length >= 3) {
      blocks = parts.slice(1).map((p) => "Available" + p).slice(0, 250);
    }
  }

  const results = [];
  const seen = new Set();

  for (const b of blocks) {
    const text = stripTags(b);

    const name =
      firstMatch(text, /Floor\s*Plan\s*[:\-]\s*([A-Za-z0-9][A-Za-z0-9 \-_]+)/i) ||
      firstMatch(text, /^([A-Za-z0-9][A-Za-z0-9 \-_]{1,40})\s+(Studio|[0-9]\s*Bed)/i) ||
      firstMatch(text, /^([A-Za-z0-9][A-Za-z0-9 \-_]{1,40})\s+\$/) ||
      firstMatch(text, /^([A-Za-z0-9][A-Za-z0-9 \-_]{1,40})\s+[0-9]\s*Bed/i);

    const beds = toNumberMaybe(firstMatch(text, /([0-9])\s*Bed/i)) ?? (text.match(/\bStudio\b/i) ? 0 : null);
    const baths = toNumberMaybe(firstMatch(text, /([0-9](?:\.[0-9])?)\s*Bath/i));
    const sqft = toNumberMaybe(firstMatch(text, /([0-9,]{3,5})\s*(?:sq\.?\s*ft|sqft|sq\s*ft)/i));
    const availableCount = toNumberMaybe(firstMatch(text, /([0-9]{1,3})\s+Available/i));

    const priceText =
      firstMatch(text, /(\$[0-9,]+(?:\s*-\s*\$[0-9,]+)?)/) || firstMatch(text, /(Starting\s+at\s+\$[0-9,]+)/i);

    if (!name && beds == null && baths == null && sqft == null && !priceText && availableCount == null) continue;

    const key = `${name || "unknown"}|${beds}|${baths}|${sqft}|${priceText || ""}|${availableCount || ""}`;
    if (seen.has(key)) continue;
    seen.add(key);

    results.push({
      unit_key: null,
      unit_id: `floorplan:${name || "unknown"}:${beds ?? "x"}:${baths ?? "x"}:${sqft ?? "x"}`,
      unit_number: null,
      available_on: null,
      price: null,
      floor_plan_id: null,
      meta: {
        beds,
        baths,
        sqft,
        available_count: availableCount,
        price_text: priceText || null,
        source: "RentCafe",
        page_url: pageUrl,
        kind: "floorplan_fallback",
      },
    });
  }

  return results;
}

// ================== OPTIONAL JSON SNIFFER ==================
function attachJsonSniffer(page) {
  let saved = 0;

  const isProbablyNoiseHost = (host) => {
    const h = host.toLowerCase();
    return (
      h.includes("cookielaw.org") ||
      h.includes("cookie") ||
      h.includes("googletagmanager") ||
      h.includes("google-analytics") ||
      h.includes("doubleclick") ||
      h.includes("facebook") ||
      h.includes("hotjar") ||
      h.includes("meetelise")
    );
  };

  const looksLikeInventoryUrl = (url) => {
    const u = url.toLowerCase();
    return (
      u.includes("rentcafe") ||
      u.includes("securecafe") ||
      u.includes("onlineleasing") ||
      u.includes("availability") ||
      u.includes("available") ||
      u.includes("floorplan") ||
      u.includes("floorplans") ||
      u.includes("unit") ||
      u.includes("units") ||
      u.includes("apartments") ||
      u.includes("inventory") ||
      u.includes("property") ||
      u.includes("search")
    );
  };

  page.on("response", async (res) => {
    try {
      const ct = (res.headers()["content-type"] || "").toLowerCase();
      if (!ct.includes("application/json") && !ct.includes("text/json")) return;

      const url = res.url();
      const host = new URL(url).host;

      if (isProbablyNoiseHost(host)) return;
      if (!looksLikeInventoryUrl(url)) return;

      const body = await res.text();
      if (!body || body.length < 300) return;

      saved += 1;
      if (saved <= 20) {
        const safeHost = host.replaceAll(".", "_");
        const file = `rentcafe_json_${String(saved).padStart(2, "0")}_${safeHost}.txt`;
        fs.writeFileSync(file, `URL: ${url}\n\n${body}`, "utf8");
        console.log("Saved JSON:", file);
      }
    } catch {}
  });
}

// ================== PLAYWRIGHT (SHARED) ==================
async function createBrowserAndContext() {
  const browser = await chromium.launch({ headless: HEADLESS, slowMo: SLOW_MO_MS });
  const context = await browser.newContext({
    userAgent:
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    viewport: { width: 1280, height: 800 },
    locale: "en-US",
  });
  const page = await context.newPage();

  await page.addInitScript(() => {
    Object.defineProperty(navigator, "webdriver", { get: () => false });
  });

  if (RENTCAFE_SNIFF_JSON) attachJsonSniffer(page);

  return { browser, context, page };
}

async function gotoWithRetries(page, url) {
  let lastErr = null;

  for (let attempt = 0; attempt <= NAV_RETRIES; attempt++) {
    try {
      console.log(`Opening: ${url} (attempt ${attempt + 1}/${NAV_RETRIES + 1})`);
      const resp = await page.goto(url, { waitUntil: WAIT_UNTIL, timeout: GOTO_TIMEOUT_MS });
      console.log("HTTP status:", resp ? resp.status() : null);

      // settle
      await page.waitForTimeout(SETTLE_MS);
      return resp;
    } catch (err) {
      lastErr = err;
      console.log("⚠️ goto failed:", err?.message || err);

      // quick backoff
      await page.waitForTimeout(1500 + attempt * 1000);
    }
  }

  // debug artifacts
  const base = safeFilename(url);
  try {
    await page.screenshot({ path: `rentcafe_fail_${base}.png`, fullPage: true });
    fs.writeFileSync(`rentcafe_fail_${base}.html`, await page.content(), "utf8");
    console.log("Saved debug:", `rentcafe_fail_${base}.png / .html`);
  } catch {}

  throw lastErr || new Error("gotoWithRetries failed");
}

async function fetchPagePayload(page, url) {
  await gotoWithRetries(page, url);

  const domUnits = isSecureCafeUrl(url)
    ? await extractAvailableUnitsFromSecureCafe(page, url)
    : await extractAvailableUnitsFromDom(page, url);

  const html = await page.content();
  return { url, domUnits, html };
}

// ================== SUPABASE IO ==================
async function getLatestSnapshot(propertyId, upToDateISO) {
  const { data, error } = await supabase
    .from("unit_snapshots")
    .select("snapshot_date, units_json")
    .eq("property_id", propertyId)
    .lte("snapshot_date", upToDateISO)
    .order("snapshot_date", { ascending: false })
    .limit(1);

  if (error) throw error;
  return data?.[0] || null;
}

async function upsertSnapshot({ propertyId, snapshotDate, units }) {
  const { data, error } = await supabase
    .from("unit_snapshots")
    .upsert(
      { property_id: propertyId, snapshot_date: snapshotDate, units_json: units },
      { onConflict: "property_id,snapshot_date" }
    )
    .select("id, property_id, snapshot_date, created_at")
    .single();

  if (error) throw error;
  return data;
}

async function replaceEventsForDay({ propertyId, eventDate, source, events }) {
  const { error: delErr } = await supabase
    .from("unit_events")
    .delete()
    .eq("property_id", propertyId)
    .eq("event_date", eventDate)
    .eq("source", source);

  if (delErr) throw delErr;

  if (!events.length) return;

  const { error: insErr } = await supabase.from("unit_events").insert(events);
  if (insErr) throw insErr;
}

// ================== MAIN ==================
async function main() {
  const urls = normalizeUrlList();
  console.log("Fetching RentCafe data...");
  console.log("URLs:", urls);
  console.log("Headless:", HEADLESS);
  console.log("waitUntil:", WAIT_UNTIL);
  console.log("goto timeout ms:", GOTO_TIMEOUT_MS);
  console.log("nav retries:", NAV_RETRIES);
  console.log("Floorplan fallback enabled:", RENTCAFE_FLOORPLAN_FALLBACK);

  const snapshotDate = SNAPSHOT_DATE || todayYYYYMMDD();

  const { browser, context, page } = await createBrowserAndContext();

  const pages = [];
  try {
    for (const u of urls) {
      const payload = await fetchPagePayload(page, u);
      pages.push(payload);
      await sleep(PER_URL_DELAY_MS);
    }
  } finally {
    await page.close().catch(() => {});
    await context.close().catch(() => {});
    await browser.close().catch(() => {});
  }

  // Build “today units” (unit-level only)
  const unitsToday = [];
  const seenKeys = new Set();

  const byPage = {};
  for (const p of pages) {
    const list = Array.isArray(p.domUnits) ? p.domUnits : [];
    byPage[p.url] = list.filter((x) => x?.unit_key).length;

    for (const u of list) {
      if (!u?.unit_key) continue;

      const unit_key = normalizeUnitKeyFromNumber(u.unit_number) || u.unit_key;
      const unit_number = normalizeUnitNumber(u.unit_number) || unitNumberFromUnitKey(unit_key);
      if (!unit_key) continue;

      if (seenKeys.has(unit_key)) continue;
      seenKeys.add(unit_key);

      unitsToday.push({ ...u, unit_key, unit_number });
    }
  }

  console.log("Units by page_url:", byPage);
  console.log(`Units extracted: ${unitsToday.length}`);

  // optional fallback (does not affect unit_events because not unit-level)
  if (unitsToday.length === 0 && RENTCAFE_FLOORPLAN_FALLBACK) {
    let fpCount = 0;
    for (const p of pages) {
      const fp = parseFloorplansFromHtml(p.html || "", p.url);
      fpCount += fp.length;
    }
    console.log(`(Fallback) floorplan rows extracted: ${fpCount}`);
  }

  // IMPORTANT CHANGE: if 0 units but pages loaded, we STILL write snapshot (empty)
  const prevSnap = await getLatestSnapshot(RENTCAFE_PROPERTY_ID, snapshotDate);
  const prevUnitsRaw = prevSnap?.units_json || [];

  const prevUnits = (prevUnitsRaw || [])
    .map((u) => {
      const unit_key =
        u?.unit_key ||
        normalizeUnitKeyFromNumber(u?.unit_number) ||
        (u?.unit_id?.startsWith("unit:") ? u.unit_id : null);

      const unit_number =
        u?.unit_number ||
        unitNumberFromUnitKey(unit_key) ||
        (u?.unit_id?.startsWith("unit:") ? u.unit_id.slice(5) : null);

      return { ...u, unit_key, unit_number };
    })
    .filter((u) => u.unit_key);

  const { appeared, disappeared } = diffKeys(prevUnits, unitsToday);

  const unitKeyToNumber = new Map();
  for (const u of unitsToday) {
    if (u.unit_key && u.unit_number) unitKeyToNumber.set(u.unit_key, String(u.unit_number));
  }

  const events = [
    ...appeared.map((k) => ({
      property_id: RENTCAFE_PROPERTY_ID,
      event_date: snapshotDate,
      unit_key: k,
      unit_number: unitKeyToNumber.get(k) || unitNumberFromUnitKey(k),
      event_type: "appeared",
      source: EVENT_SOURCE,
    })),
    ...disappeared.map((k) => ({
      property_id: RENTCAFE_PROPERTY_ID,
      event_date: snapshotDate,
      unit_key: k,
      unit_number: unitKeyToNumber.get(k) || unitNumberFromUnitKey(k),
      event_type: "disappeared",
      source: EVENT_SOURCE,
    })),
  ];

  console.log(
    `Diff results for ${snapshotDate}: +${appeared.length} appeared, -${disappeared.length} disappeared`
  );

  console.log("Pushing snapshot to Supabase...");
  const row = await upsertSnapshot({
    propertyId: RENTCAFE_PROPERTY_ID,
    snapshotDate,
    units: unitsToday, // can be empty (valid)
  });

  console.log(`Writing unit_events (source=${EVENT_SOURCE})...`);
  await replaceEventsForDay({
    propertyId: RENTCAFE_PROPERTY_ID,
    eventDate: snapshotDate,
    source: EVENT_SOURCE,
    events,
  });

  console.log("Snapshot upserted:", row);
  console.log(`Events written: ${events.length}`);
}

main()
  .then(() => {
    process.exitCode = 0;
  })
  .catch((err) => {
    console.error("Failed:", err?.message || err);
    process.exitCode = 1;
  });
