// push_rentcafe_snapshot_to_supabase_v4.js
// RentCafe adapter: AVAILABLE-UNITS first (DOM), optional floorplan fallback (off by default)
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { chromium } from "playwright";
import fs from "fs";

// ====== ENV ======
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const RENTCAFE_PROPERTY_ID = process.env.RENTCAFE_PROPERTY_ID;

// Support either a single URL or a comma-separated list
const RENTCAFE_URL = process.env.RENTCAFE_URL;
const RENTCAFE_URLS = process.env.RENTCAFE_URLS; // comma-separated list

// Debug: save JSON responses while sniffing
const RENTCAFE_SNIFF_JSON =
  String(process.env.RENTCAFE_SNIFF_JSON || "").toLowerCase() === "true";

// IMPORTANT: floorplan fallback OFF by default (prevents fake “units” from floorplan cards)
const RENTCAFE_ENABLE_FLOORPLAN_FALLBACK =
  String(process.env.RENTCAFE_ENABLE_FLOORPLAN_FALLBACK || "").toLowerCase() === "true";

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in .env");
  process.exit(1);
}
if (!RENTCAFE_PROPERTY_ID) {
  console.error("Missing RENTCAFE_PROPERTY_ID in .env");
  process.exit(1);
}
if (!RENTCAFE_URL && !RENTCAFE_URLS) {
  console.error("Missing RENTCAFE_URL or RENTCAFE_URLS in .env");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

// ====== HELPERS ======
function todayYYYYMMDD() {
  const d = new Date();
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
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

function firstMatch(text, regex) {
  const m = text.match(regex);
  return m ? m[1] : null;
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

function bedsHintFromUrl(url) {
  const u = String(url || "").toLowerCase();
  if (u.includes("studio")) return 0;
  if (u.includes("single")) return 0;
  if (u.includes("one-bedroom") || u.includes("1-bedroom") || u.includes("1bed")) return 1;
  if (u.includes("two-bedroom") || u.includes("2-bedroom") || u.includes("2bed")) return 2;
  if (u.includes("three-bedroom") || u.includes("3-bedroom") || u.includes("3bed")) return 3;
  return null;
}

function parseDateAvailableToISO(raw) {
  const s = String(raw || "").trim();
  const m = s.match(/\b(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})\b/);
  if (!m) return null;
  const mm = String(m[1]).padStart(2, "0");
  const dd = String(m[2]).padStart(2, "0");
  const yyyy = m[3];
  return `${yyyy}-${mm}-${dd}`;
}

function stripTags(html) {
  return html
    .replace(/<script[\s\S]*?<\/script>/gi, "")
    .replace(/<style[\s\S]*?<\/style>/gi, "")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

// ====== DOM UNITS EXTRACTION (STRICT) ======
async function extractAvailableUnitsFromDom(page, pageUrl) {
  await Promise.race([
    page.waitForSelector("#availApts", { timeout: 4500 }).catch(() => null),
    page.waitForSelector('[id*="avail"]', { timeout: 4500 }).catch(() => null),
    page.waitForSelector("text=Available Units", { timeout: 4500 }).catch(() => null),
  ]);

  const bedsHint = bedsHintFromUrl(pageUrl);

  const rawBlocks = await page.evaluate(() => {
    const root =
      document.querySelector("#availApts") ||
      document.querySelector('[id*="avail"]') ||
      document.body;

    const candidates = Array.from(
      root.querySelectorAll(
        "li, .row, .card, .available, [class*='avail'], [class*='unit'], [class*='apartment']"
      )
    );

    const blocks = candidates
      .map((el) => (el.innerText || "").replace(/\s+/g, " ").trim())
      .filter((t) => t.length >= 20);

    const seen = new Set();
    const out = [];
    for (const b of blocks) {
      const key = b.slice(0, 180);
      if (seen.has(key)) continue;
      seen.add(key);
      out.push(b);
      if (out.length >= 250) break;
    }
    return out;
  });

  const results = [];
  const seen = new Set();

  for (const block of rawBlocks) {
    const lower = block.toLowerCase();

    // Skip waitlist/inquire/call blocks
    if (
      lower.includes("add to waitlist") ||
      lower.includes("waitlist") ||
      lower.includes("inquire for details") ||
      lower.includes("call for details") ||
      lower.includes("contact us")
    ) {
      continue;
    }

    const unitNum =
      firstMatch(block, /Apartment\s*:\s*#\s*([A-Za-z0-9-]+)/i) ||
      firstMatch(block, /Apartment\s*#\s*([A-Za-z0-9-]+)/i) ||
      firstMatch(block, /\bUnit\s*#?\s*([A-Za-z0-9-]+)/i) ||
      firstMatch(block, /\b#\s*([A-Za-z0-9-]{2,20})\b/);

    // REQUIRE unit number (prevents floorplan cards from being counted)
    if (!unitNum) continue;

    const priceText =
      firstMatch(block, /Starting\s+at\s*:\s*(\$[0-9,]+)/i) ||
      firstMatch(block, /(\$[0-9,]+)/);

    const dateAvailRaw = firstMatch(
      block,
      /Date\s*Available\s*:\s*([0-9]{1,2}[\/\-][0-9]{1,2}[\/\-][0-9]{4})/i
    );
    const dateAvailIso = dateAvailRaw ? parseDateAvailableToISO(dateAvailRaw) : null;

    const availableText =
      dateAvailIso ||
      firstMatch(block, /(Available\s+Now)/i) ||
      null;

    // Require at least price or availability
    if (!priceText && !availableText) continue;

    const key = `${unitNum}|${priceText || ""}|${availableText || ""}`;
    if (seen.has(key)) continue;
    seen.add(key);

    results.push({
      unit_id: `unit:${unitNum}`,
      unit_number: unitNum,
      available_on: availableText || null,
      price: priceText ? parseMoneyToNumber(priceText) : null,
      floor_plan_id: null,
      meta: {
        source: "RentCafe",
        page_url: pageUrl,
        beds_hint: bedsHint,
        price_text: priceText || null,
        raw: block,
      },
    });
  }

  return results;
}

// ====== FLOORPLAN FALLBACK (OPTIONAL) ======
function parseFloorplansFromHtml(html) {
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

    const beds =
      toNumberMaybe(firstMatch(text, /([0-9])\s*Bed/i)) ??
      (text.match(/\bStudio\b/i) ? 0 : null);

    const baths = toNumberMaybe(firstMatch(text, /([0-9](?:\.[0-9])?)\s*Bath/i));

    const sqft = toNumberMaybe(
      firstMatch(text, /([0-9,]{3,5})\s*(?:sq\.?\s*ft|sqft|sq\s*ft)/i)
    );

    const availableCount = toNumberMaybe(firstMatch(text, /([0-9]{1,3})\s+Available/i));

    const priceText =
      firstMatch(text, /(\$[0-9,]+(?:\s*-\s*\$[0-9,]+)?)/) ||
      firstMatch(text, /(Starting\s+at\s+\$[0-9,]+)/i);

    if (!name && beds == null && baths == null && sqft == null && !priceText && availableCount == null) {
      continue;
    }

    const key = `${name || "unknown"}|${beds}|${baths}|${sqft}|${priceText || ""}|${availableCount || ""}`;
    if (seen.has(key)) continue;
    seen.add(key);

    results.push({
      unit_id: `floorplan:${name || "unknown"}:${beds ?? "x"}:${baths ?? "x"}:${sqft ?? "x"}`,
      unit_number: name || null,
      available_on: null,
      price: null,
      floor_plan_id: null,
      meta: {
        source: "RentCafe",
        beds,
        baths,
        sqft,
        available_count: availableCount,
        price_text: priceText || null,
      },
    });
  }

  return results;
}

// ====== OPTIONAL JSON SNIFFER (debug only) ======
function attachJsonSniffer(page) {
  let saved = 0;

  page.on("response", async (res) => {
    try {
      const ct = (res.headers()["content-type"] || "").toLowerCase();
      if (!ct.includes("application/json") && !ct.includes("text/json")) return;

      const url = res.url();
      const body = await res.text();
      if (!body || body.length < 300) return;

      saved += 1;
      if (saved <= 10) {
        const host = new URL(url).host.replaceAll(".", "_");
        const file = `rentcafe_json_${String(saved).padStart(2, "0")}_${host}.txt`;
        fs.writeFileSync(file, `URL: ${url}\n\n${body}`, "utf8");
        console.log("Saved JSON:", file);
      }
    } catch {}
  });
}

// ====== FETCH PAGE ======
async function fetchRentCafePage(url) {
  const browser = await chromium.launch({ headless: true });

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

  try {
    console.log("Opening:", url);
    const resp = await page.goto(url, { waitUntil: "domcontentloaded", timeout: 60000 });
    console.log("HTTP status:", resp ? resp.status() : null);

    await page.waitForTimeout(2500);

    const domUnits = await extractAvailableUnitsFromDom(page, url);
    const html = await page.content();

    return { url, domUnits, html };
  } finally {
    await page.close().catch(() => {});
    await context.close().catch(() => {});
    await browser.close().catch(() => {});
  }
}

// ====== PIPELINE ======
async function fetchRentCafePayload() {
  const urls = normalizeUrlList();
  const pages = [];
  for (const u of urls) pages.push(await fetchRentCafePage(u));
  return { pages };
}

function extractUnits(payload) {
  const pages = payload?.pages || [];
  const all = [];
  const seen = new Set();

  for (const p of pages) {
    // Use DOM units only
    if (Array.isArray(p.domUnits) && p.domUnits.length) {
      for (const u of p.domUnits) {
        if (!u?.unit_id) continue;
        if (seen.has(u.unit_id)) continue;
        seen.add(u.unit_id);
        all.push(u);
      }
      continue;
    }

    // Optional floorplan fallback (OFF by default)
    if (RENTCAFE_ENABLE_FLOORPLAN_FALLBACK) {
      const html = p?.html || "";
      const fp = html ? parseFloorplansFromHtml(html) : [];
      for (const u of fp) {
        if (!u?.unit_id) continue;
        if (seen.has(u.unit_id)) continue;
        seen.add(u.unit_id);
        // tag page_url for debugging
        u.meta = { ...(u.meta || {}), page_url: p.url };
        all.push(u);
      }
    }
  }

  return all;
}

async function upsertSnapshot({ propertyId, snapshotDate, units }) {
  const { data, error } = await supabase
    .from("unit_snapshots")
    .upsert(
      {
        property_id: propertyId,
        snapshot_date: snapshotDate,
        units_json: units,
      },
      { onConflict: "property_id,snapshot_date" }
    )
    .select("id, property_id, snapshot_date, created_at")
    .single();

  if (error) throw error;
  return data;
}

async function main() {
  const urls = normalizeUrlList();
  console.log("Fetching RentCafe data...");
  console.log("URLs:", urls);
  console.log("Floorplan fallback enabled:", RENTCAFE_ENABLE_FLOORPLAN_FALLBACK);

  const payload = await fetchRentCafePayload();
  const units = extractUnits(payload);

  const byPage = {};
  for (const u of units) {
    const k = u?.meta?.page_url || "unknown";
    byPage[k] = (byPage[k] || 0) + 1;
  }
  console.log("Units by page_url:", byPage);
  console.log(`Units extracted: ${units.length}`);

  if (units.length === 0) {
    console.log("⚠️ Extracted 0 available units.");
    console.log("Tip: set RENTCAFE_SNIFF_JSON=true to save JSON responses for debugging.");
    console.log("If you WANT floorplans instead, set RENTCAFE_ENABLE_FLOORPLAN_FALLBACK=true.");
    process.exit(2);
  }

  const snapshotDate = process.env.SNAPSHOT_DATE || todayYYYYMMDD();

  console.log("Pushing snapshot to Supabase...");
  const row = await upsertSnapshot({
    propertyId: RENTCAFE_PROPERTY_ID,
    snapshotDate,
    units,
  });

  console.log("✅ Snapshot upserted:", row);
}

main()
  .then(() => setTimeout(() => process.exit(0), 200))
  .catch((err) => {
    console.error("❌ Failed:", err?.message || err);
    setTimeout(() => process.exit(1), 200);
  });
