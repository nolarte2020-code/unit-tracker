// run_rentcafe_all_with_events.js
// Mode A (single property): uses .env RENTCAFE_PROPERTY_ID + RENTCAFE_URLS
// Mode B (all properties): pulls platform='RentCafe' from Supabase (fallback)
// Writes unit_snapshots + unit_events (so daily views update)

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { chromium } from "playwright";

// ===== ENV =====
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

// single-property mode (preferred when called from PS foreach)
const RENTCAFE_PROPERTY_ID = process.env.RENTCAFE_PROPERTY_ID || "";
const RENTCAFE_URLS_ENV = process.env.RENTCAFE_URLS || ""; // comma-separated

// Use same convention as diff script / view-by-source
const EVENT_SOURCE = process.env.EVENT_SOURCE || process.env.RENTCAFE_EVENT_SOURCE || "snapshot";

// Optional: limit for testing in ALL-properties mode
const LIMIT = Number(process.env.RENTCAFE_LIMIT || 0);

// Optional: override date
const SNAPSHOT_DATE = process.env.SNAPSHOT_DATE || "";

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in .env");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

// ===== Date helpers (LOCAL date) =====
function todayYYYYMMDDLocal() {
  const d = new Date();
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

// ===== helpers =====
function parseCsvUrls(raw) {
  return [...new Set(
    String(raw || "")
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean)
  )];
}

function normalizeUrlListFromRow(row) {
  const raw =
    row?.rentcafe_urls && String(row.rentcafe_urls).trim()
      ? String(row.rentcafe_urls)
      : row?.url
      ? String(row.url)
      : "";

  return parseCsvUrls(raw);
}

function parseMoneyToNumber(s) {
  if (!s) return null;
  const cleaned = String(s).replace(/[^\d.]/g, "");
  if (!cleaned) return null;
  const n = Number(cleaned);
  return Number.isFinite(n) ? n : null;
}

function firstMatch(text, regex) {
  const m = text.match(regex);
  return m ? m[1] : null;
}

// ===== RentCafe DOM-only extractor (strict) =====
async function extractAvailableUnitsFromDom(page, pageUrl) {
  await page.waitForTimeout(1200);
  await page.waitForSelector("#availApts", { timeout: 4000 }).catch(() => null);

  const rawBlocks = await page.evaluate(() => {
    const root = document.querySelector("#availApts") || document.body;

    const candidates = Array.from(
      root.querySelectorAll(
        "li, .row, .card, .available, [class*='avail'], [class*='unit']"
      )
    );

    const blocks = candidates
      .map((el) => el.innerText || "")
      .map((t) => t.replace(/\s+/g, " ").trim())
      .filter((t) => t.length >= 20);

    const seen = new Set();
    const out = [];
    for (const b of blocks) {
      const key = b.slice(0, 220);
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

    if (
      lower.includes("add to waitlist") ||
      lower.includes("waitlist") ||
      lower.includes("inquire for details") ||
      lower.includes("call for details") ||
      lower.includes("contact us") ||
      lower.includes("join waitlist")
    ) {
      continue;
    }

    const unitNum =
      firstMatch(block, /Apartment\s*#?\s*([A-Za-z0-9-]+)/i) ||
      firstMatch(block, /\bUnit\s*#?\s*([A-Za-z0-9-]+)/i) ||
      firstMatch(block, /\b#\s*([A-Za-z0-9-]{2,12})\b/);

    if (!unitNum) continue;

    const priceText =
      firstMatch(block, /Starting\s+at\s*:\s*(\$[0-9,]+)/i) ||
      firstMatch(block, /(\$[0-9,]+)/);

    const availableText =
      firstMatch(
        block,
        /(Available\s+(?:Now|[A-Za-z]{3,9}\s+[0-9]{1,2}(?:st|nd|rd|th)?))/i
      ) || (/\bAvailable\b/i.test(block) ? "Available" : null);

    const key = `${unitNum}|${priceText || ""}|${availableText || ""}|${pageUrl}`;
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
        price_text: priceText || null,
        raw: block,
      },
    });
  }

  return results;
}

async function scrapeRentCafeUrls(urls) {
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

  const all = [];
  const seen = new Set();

  try {
    for (const url of urls) {
      console.log("Opening:", url);
      const resp = await page.goto(url, {
        waitUntil: "domcontentloaded",
        timeout: 60000,
      });
      console.log("HTTP status:", resp ? resp.status() : null);

      await page.waitForTimeout(2200);

      const units = await extractAvailableUnitsFromDom(page, url);

      for (const u of units) {
        const k = u.unit_id || `${u?.meta?.page_url || ""}|${u.unit_number || ""}`;
        if (!k) continue;
        if (seen.has(k)) continue;
        seen.add(k);
        all.push(u);
      }
    }
  } finally {
    await page.close().catch(() => {});
    await context.close().catch(() => {});
    await browser.close().catch(() => {});
  }

  return all;
}

// ===== Snapshot + Events =====
function unitKeyForEvents(u) {
  return String(u?.unit_id || u?.unit_number || "").trim() || null;
}

function diffUnitKeys(prevUnits, currUnits) {
  const prevKeys = new Set((prevUnits || []).map(unitKeyForEvents).filter(Boolean));
  const currKeys = new Set((currUnits || []).map(unitKeyForEvents).filter(Boolean));

  const appeared = [...currKeys].filter((k) => !prevKeys.has(k));
  const disappeared = [...prevKeys].filter((k) => !currKeys.has(k));

  return { appeared, disappeared };
}

async function upsertSnapshot(propertyId, snapshotDate, units) {
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

async function getPreviousSnapshot(propertyId, today) {
  const { data, error } = await supabase
    .from("unit_snapshots")
    .select("snapshot_date, units_json")
    .eq("property_id", propertyId)
    .lt("snapshot_date", today)
    .order("snapshot_date", { ascending: false })
    .limit(1);

  if (error) throw error;
  return data?.[0] || null;
}

async function writeEvents(propertyId, today, appeared, disappeared) {
  const { error: delErr } = await supabase
    .from("unit_events")
    .delete()
    .eq("property_id", propertyId)
    .eq("event_date", today)
    .eq("source", EVENT_SOURCE);

  if (delErr) throw delErr;

  const events = [
    ...appeared.map((unit_key) => ({
      property_id: propertyId,
      event_date: today,
      unit_key,
      unit_number: unit_key?.startsWith("unit:") ? unit_key.slice(5) : null,
      event_type: "appeared",
      source: EVENT_SOURCE,
    })),
    ...disappeared.map((unit_key) => ({
      property_id: propertyId,
      event_date: today,
      unit_key,
      unit_number: unit_key?.startsWith("unit:") ? unit_key.slice(5) : null,
      event_type: "disappeared",
      source: EVENT_SOURCE,
    })),
  ];

  if (events.length === 0) return { inserted: 0 };

  const { error: insErr } = await supabase.from("unit_events").insert(events);
  if (insErr) throw insErr;

  return { inserted: events.length };
}

async function runOneProperty({ id, name, urls }) {
  const today = SNAPSHOT_DATE || todayYYYYMMDDLocal();
  console.log("Today:", today);
  console.log("Event source:", EVENT_SOURCE);

  console.log("\n==============================");
  console.log("Property:", name || "(env)", "|", id);
  console.log("URLs:", urls);

  if (!urls || urls.length === 0) {
    console.log("⚠️ No URLs found. Skipping.");
    return;
  }

  const units = await scrapeRentCafeUrls(urls);

  const counts = {};
  for (const u of units) {
    const k = u?.meta?.page_url || "unknown";
    counts[k] = (counts[k] || 0) + 1;
  }
  console.log("Units by page_url:", counts);
  console.log("Units extracted:", units.length);

  const snap = await upsertSnapshot(id, today, units);
  console.log("✅ Snapshot upserted:", snap.id);

  const prev = await getPreviousSnapshot(id, today);
  const prevUnits = prev?.units_json || [];
  const { appeared, disappeared } = diffUnitKeys(prevUnits, units);

  const ev = await writeEvents(id, today, appeared, disappeared);
  console.log(
    `✅ Events written: inserted=${ev.inserted} (appeared=${appeared.length}, disappeared=${disappeared.length})`
  );
}

async function main() {
  // --- MODE A: Single-property mode if env has RENTCAFE_PROPERTY_ID ---
  if (RENTCAFE_PROPERTY_ID) {
    const urls = parseCsvUrls(RENTCAFE_URLS_ENV);

    // If RENTCAFE_URLS isn’t set, you can still fallback to reading property.url from DB
    let finalUrls = urls;
    let propName = "RentCafe (single)";

    if (finalUrls.length === 0) {
      const { data, error } = await supabase
        .from("properties")
        .select("id, name, url, rentcafe_urls")
        .eq("id", RENTCAFE_PROPERTY_ID)
        .single();

      if (error) throw error;

      propName = data?.name || propName;
      finalUrls = normalizeUrlListFromRow(data);
    }

    await runOneProperty({
      id: RENTCAFE_PROPERTY_ID,
      name: propName,
      urls: finalUrls,
    });

    console.log("\n✅ Done (single property).");
    return;
  }

  // --- MODE B: All-properties fallback ---
  const today = SNAPSHOT_DATE || todayYYYYMMDDLocal();
  console.log("Today:", today);
  console.log("Event source:", EVENT_SOURCE);

  let q = supabase
    .from("properties")
    .select("id, name, url, platform, rentcafe_urls")
    .in("platform", ["RentCafe", "rentcafe"]) // support either stored value
    .order("created_at", { ascending: false });

  if (LIMIT > 0) q = q.limit(LIMIT);

  const { data: props, error } = await q;
  if (error) throw error;

  if (!props || props.length === 0) {
    console.log("No RentCafe properties found (platform must equal 'RentCafe' or 'rentcafe').");
    process.exit(0);
  }

  console.log(`Found ${props.length} RentCafe properties.`);

  for (const p of props) {
    const urls = normalizeUrlListFromRow(p);
    await runOneProperty({ id: p.id, name: p.name, urls });
  }

  console.log("\n✅ All done (all properties).");
}

main()
  .then(() => setTimeout(() => process.exit(0), 200))
  .catch((err) => {
    console.error("❌ Failed:", err?.message || err);
    setTimeout(() => process.exit(1), 200);
  });
