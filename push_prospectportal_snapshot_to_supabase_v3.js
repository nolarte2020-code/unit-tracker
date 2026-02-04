// push_prospectportal_snapshot_to_supabase_v3.js
// ProspectPortal (Entrata) runner: discover floorplan pages -> extract available unit rows -> snapshot upsert -> diff -> unit_events
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { chromium } from "playwright";
import fs from "fs";

// ================== ENV ==================
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const PROPERTY_ID = process.env.PROPERTY_ID; // Supabase properties.id UUID
const PROSPECTPORTAL_URL = process.env.PROSPECTPORTAL_URL; // listing page
const PROSPECTPORTAL_URLS = process.env.PROSPECTPORTAL_URLS; // optional override list (comma-separated floorplan URLs)

const EVENT_SOURCE = process.env.EVENT_SOURCE || "prospectportal";
const SNAPSHOT_DATE = process.env.SNAPSHOT_DATE; // YYYY-MM-DD optional

const PROSPECTPORTAL_SNIFF_JSON =
  String(process.env.PROSPECTPORTAL_SNIFF_JSON || "").toLowerCase() === "true";

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in env");
  process.exit(1);
}
if (!PROPERTY_ID) {
  console.error("Missing PROPERTY_ID in env");
  process.exit(1);
}
if (!PROSPECTPORTAL_URL && !PROSPECTPORTAL_URLS) {
  console.error("Missing PROSPECTPORTAL_URL or PROSPECTPORTAL_URLS in env");
  process.exit(1);
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

function safeWriteFile(name, content) {
  try {
    fs.writeFileSync(name, content, "utf8");
    return true;
  } catch {
    return false;
  }
}

function uniq(arr) {
  return [...new Set(arr)];
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

function toNumberMaybe(s) {
  if (s == null) return null;
  const cleaned = String(s).replace(/[^\d.]/g, "");
  if (!cleaned) return null;
  const n = Number(cleaned);
  return Number.isFinite(n) ? n : null;
}

function parseMoneyToNumber(s) {
  // "$2,215/month" -> 2215
  return toNumberMaybe(s);
}

function diffKeys(prevUnits, currUnits) {
  const prevSet = new Set((prevUnits || []).map((u) => u.unit_key).filter(Boolean));
  const currSet = new Set((currUnits || []).map((u) => u.unit_key).filter(Boolean));
  const appeared = [...currSet].filter((k) => !prevSet.has(k));
  const disappeared = [...prevSet].filter((k) => !currSet.has(k));
  return { appeared, disappeared };
}

function looksLikeFloorplanUrl(url) {
  const u = String(url || "").toLowerCase();
  return (
    u.includes("prospectportal.com") &&
    u.includes("/floorplans/") &&
    u.includes("/occupancy_type/conventional/")
  );
}

// ================== COOKIE CONSENT ==================
async function acceptCookiesIfPresent(page) {
  const selectors = [
    "button:has-text('I Accept All Cookies')",
    "button:has-text('Accept All Cookies')",
    "button:has-text('I Accept Cookies')",
    "button:has-text('Accept Cookies')",
  ];

  for (const sel of selectors) {
    try {
      const btn = page.locator(sel).first();
      if (await btn.isVisible({ timeout: 1500 })) {
        await btn.click({ timeout: 2000 });
        await page.waitForTimeout(800);
        console.log("🍪 Accepted cookies");
        return true;
      }
    } catch {}
  }
  return false;
}

// ================== OPTIONAL JSON SNIFFER (ProspectPortal host only) ==================
function attachJsonSniffer(page) {
  page.on("response", async (res) => {
    try {
      const ct = (res.headers()["content-type"] || "").toLowerCase();
      if (!ct.includes("application/json") && !ct.includes("text/json")) return;

      const url = res.url();
      const host = new URL(url).host.toLowerCase();
      if (!host.endsWith("prospectportal.com")) return;

      console.log("JSON:", url);

      const body = await res.text();
      if (!body || body.length < 200) return;

      const safeHost = host.replaceAll(".", "_");
      const safe = url.replace(/[^a-z0-9]+/gi, "_").slice(0, 120);
      const file = `pp_json_${safeHost}_${safe}.txt`;
      safeWriteFile(file, `URL: ${url}\n\n${body}`);
    } catch {}
  });
}

// ================== DISCOVERY ==================
async function discoverFloorplanUrls(page, listingUrl) {
  const links = await page.evaluate(() => {
    const anchors = Array.from(document.querySelectorAll("a[href]"));
    return anchors.map((a) => a.getAttribute("href") || "").filter(Boolean);
  });

  const base = new URL(listingUrl);
  const out = [];

  for (const h of links) {
    try {
      const abs = new URL(h, base).toString();
      if (looksLikeFloorplanUrl(abs)) out.push(abs);
    } catch {}
  }

  return uniq(out);
}

// ================== EXTRACTION ==================
// Table-first extraction that matches your screenshot (Unit / Building / Rent / Sq.Ft. / Deposit / Available)
async function extractUnitsFromFloorplanPage(page, pageUrl) {
  // Ensure page is stable and scrolled to trigger lazy content
  await page.waitForTimeout(800);

  // Scroll down to where the Available Units table is
  for (let i = 0; i < 4; i++) {
    await page.mouse.wheel(0, 1400).catch(() => {});
    await page.waitForTimeout(500);
  }

  // Wait for the Available Units area and table
  await page.waitForSelector("text=/Available Units/i", { timeout: 15000 }).catch(() => null);
  await page.waitForSelector("table tr", { timeout: 15000 }).catch(() => null);
  await page.waitForTimeout(800);

  // Debug screenshot + html (optional, but helpful)
  try {
    await page.screenshot({ path: "pp_debug_floorplan.png", fullPage: true });
    console.log("Saved screenshot: pp_debug_floorplan.png");
  } catch {}

  try {
    const safe = pageUrl.replace(/[^a-z0-9]+/gi, "_").slice(0, 90);
    const html = await page.content();
    safeWriteFile(`pp_floorplan_${safe}.html`, html);
  } catch {}

  // Pull rows from the table in/near "Available Units"
  const rows = await page.evaluate((pageUrl) => {
    const norm = (s) => String(s || "").replace(/\u00a0/g, " ").replace(/\s+/g, " ").trim();

    // Find heading node that contains "Available Units"
    const heading = Array.from(document.querySelectorAll("h1,h2,h3,h4,h5,div,span,p")).find((el) =>
      /available units/i.test(norm(el.innerText))
    );

    const scope = heading ? (heading.closest("section,div") || document.body) : document.body;
    const table = scope.querySelector("table") || document.querySelector("table");
    if (!table) return [];

    const trs = Array.from(table.querySelectorAll("tr"));
    const out = [];

    for (const tr of trs) {
      const t = norm(tr.innerText);
      if (!t) continue;

      // Skip header row
      if (/^unit\b/i.test(t) && /\brent\b/i.test(t) && /\bavailable\b/i.test(t)) continue;

      const tds = Array.from(tr.querySelectorAll("td")).map((td) => norm(td.innerText));
      if (tds.length < 2) continue;

      // Column mapping based on your screenshot:
      // 0 Unit, 1 Building, 2 Rent, 3 SqFt, 4 Deposit, 5 Available
      const unit = tds[0] || null;
      const building = tds[1] || null;
      const rent = tds[2] || null;
      const sqft = tds[3] || null;
      const deposit = tds[4] || null;
      const available = tds[5] || null;

      // Unit should be something like 1614, 1314, etc.
      if (!unit || !/^[A-Za-z0-9-]{1,15}$/.test(unit)) continue;

      out.push({
        unit_number: unit,
        building,
        rent_text: rent,
        sqft_text: sqft,
        deposit_text: deposit,
        available_text: available,
        raw: t,
        page_url: pageUrl,
      });
    }

    // Dedup by unit number
    const seen = new Set();
    const deduped = [];
    for (const r of out) {
      const k = String(r.unit_number || "").toLowerCase();
      if (!k || seen.has(k)) continue;
      seen.add(k);
      deduped.push(r);
    }
    return deduped.slice(0, 500);
  }, pageUrl);

  const results = [];
  const seenKeys = new Set();

  for (const r of rows) {
    const unit_number = normalizeUnitNumber(r.unit_number);
    const unit_key = normalizeUnitKeyFromNumber(unit_number);
    if (!unit_key) continue;

    if (seenKeys.has(unit_key)) continue;
    seenKeys.add(unit_key);

    // Parse rent like "From $2,215/month" -> 2215
    let price = null;
    if (r.rent_text) {
      const m = String(r.rent_text).match(/\$[0-9,]{3,7}/);
      if (m) price = parseMoneyToNumber(m[0]);
    }

    results.push({
      unit_key,
      unit_id: unit_key,
      unit_number,
      available_on: r.available_text || null,
      price,
      floor_plan_id: null,
      meta: {
        source: "ProspectPortal",
        page_url: r.page_url,
        building: r.building || null,
        rent_text: r.rent_text || null,
        sqft_text: r.sqft_text || null,
        deposit_text: r.deposit_text || null,
        available_text: r.available_text || null,
        raw: r.raw || null,
      },
    });
  }

  return results;
}

// ================== PLAYWRIGHT RUN ==================
async function scrapeProspectPortalUnits() {
  const browser = await chromium.launch({ headless: true });

  const context = await browser.newContext({
    userAgent:
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    viewport: { width: 1280, height: 800 },
    locale: "en-US",
  });

  const page = await context.newPage();
  if (PROSPECTPORTAL_SNIFF_JSON) attachJsonSniffer(page);

  try {
    let floorplanUrls = [];

    if (PROSPECTPORTAL_URLS) {
      floorplanUrls = PROSPECTPORTAL_URLS.split(",").map((s) => s.trim()).filter(Boolean);
      console.log("Using PROSPECTPORTAL_URLS override. Count:", floorplanUrls.length);
    } else {
      console.log("Opening listing:", PROSPECTPORTAL_URL);
      const resp = await page.goto(PROSPECTPORTAL_URL, {
        waitUntil: "networkidle",
        timeout: 60000,
      });
      console.log("HTTP status:", resp ? resp.status() : null);

      await acceptCookiesIfPresent(page);
      await page.waitForTimeout(1000);

      const html = await page.content();
      safeWriteFile("prospectportal_listing.html", html);
      console.log("Saved listing HTML: prospectportal_listing.html");

      floorplanUrls = await discoverFloorplanUrls(page, PROSPECTPORTAL_URL);
      safeWriteFile("prospectportal_floorplan_urls.txt", floorplanUrls.join("\n"));
      console.log("Discovered floorplan URLs:", floorplanUrls.length);
      console.log("Saved: prospectportal_floorplan_urls.txt");
    }

    const allUnits = [];
    const seenKeys = new Set();

    for (const fpUrl of floorplanUrls) {
      try {
        console.log("Opening floorplan:", fpUrl);
        await page.goto(fpUrl, { waitUntil: "networkidle", timeout: 60000 });

        // 🔑 REQUIRED: cookie banner blocks table rendering in headless
        await acceptCookiesIfPresent(page);

        const units = await extractUnitsFromFloorplanPage(page, fpUrl);
        console.log("Units found on floorplan:", units.length);

        for (const u of units) {
          if (!u?.unit_key) continue;
          if (seenKeys.has(u.unit_key)) continue;
          seenKeys.add(u.unit_key);
          allUnits.push(u);
        }
      } catch {
        console.log("Skipping floorplan due to error:", fpUrl);
      }
    }

    return allUnits;
  } finally {
    await page.close().catch(() => {});
    await context.close().catch(() => {});
    await browser.close().catch(() => {});
  }
}

// ================== SNAPSHOT + EVENTS WRITES ==================
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
  const snapshotDate = SNAPSHOT_DATE || todayYYYYMMDD();

  console.log("Fetching ProspectPortal data...");
  console.log("EVENT_SOURCE:", EVENT_SOURCE);

  const unitsToday = await scrapeProspectPortalUnits();
  console.log("Units extracted:", unitsToday.length);

  if (!unitsToday.length) {
    console.log("Extracted 0 units. Open pp_debug_floorplan.png to see what's rendered.");
    process.exit(2);
  }

  const prevSnap = await getLatestSnapshot(PROPERTY_ID, snapshotDate);
  const prevUnitsRaw = prevSnap?.units_json || [];

  const prevUnits = (prevUnitsRaw || [])
    .map((u) => {
      const unit_key =
        u?.unit_key ||
        normalizeUnitKeyFromNumber(u?.unit_number) ||
        (u?.unit_id?.startsWith("unit:") ? u.unit_id : null);

      const unit_number =
        u?.unit_number || unitNumberFromUnitKey(unit_key) || unitNumberFromUnitKey(u?.unit_id);

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
      property_id: PROPERTY_ID,
      event_date: snapshotDate,
      unit_key: k,
      unit_number: unitKeyToNumber.get(k) || unitNumberFromUnitKey(k),
      event_type: "appeared",
      source: EVENT_SOURCE,
    })),
    ...disappeared.map((k) => ({
      property_id: PROPERTY_ID,
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
    propertyId: PROPERTY_ID,
    snapshotDate,
    units: unitsToday,
  });

  console.log(`Writing unit_events (source=${EVENT_SOURCE})...`);
  await replaceEventsForDay({
    propertyId: PROPERTY_ID,
    eventDate: snapshotDate,
    source: EVENT_SOURCE,
    events,
  });

  console.log("Snapshot upserted:", row);
  console.log("Events written:", events.length);
}

main()
  .then(() => (process.exitCode = 0))
  .catch((err) => {
    console.error("Failed:", err?.message || err);
    process.exitCode = 1;
  });

