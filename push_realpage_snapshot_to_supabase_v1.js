// push_realpage_snapshot_to_supabase_v1.js
// RealPage Online Leasing runner: sniff inventory JSON -> extract unit rows -> snapshot upsert -> diff -> unit_events
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { chromium } from "playwright";
import fs from "fs";

// ================== ENV ==================
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const PROPERTY_ID = process.env.PROPERTY_ID; // Supabase properties.id UUID
const REALPAGE_URL = process.env.REALPAGE_URL; // e.g. https://8179125.onlineleasing.realpage.com/#k=14423
const EVENT_SOURCE = process.env.EVENT_SOURCE || "realpage";
const SNAPSHOT_DATE = process.env.SNAPSHOT_DATE; // optional YYYY-MM-DD

// Debug toggles
const REALPAGE_SAVE_JSON =
  String(process.env.REALPAGE_SAVE_JSON || "true").toLowerCase() === "true";
const REALPAGE_HEADLESS =
  String(process.env.REALPAGE_HEADLESS || "true").toLowerCase() === "true";

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  process.exit(1);
}
if (!PROPERTY_ID) {
  console.error("Missing PROPERTY_ID");
  process.exit(1);
}
if (!REALPAGE_URL) {
  console.error("Missing REALPAGE_URL");
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

function toNumberMaybe(s) {
  if (s == null) return null;
  const cleaned = String(s).replace(/[^\d.]/g, "");
  if (!cleaned) return null;
  const n = Number(cleaned);
  return Number.isFinite(n) ? n : null;
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
  return s.startsWith("unit:") ? s.slice(5) || null : s || null;
}

function diffKeys(prevUnits, currUnits) {
  const prevSet = new Set((prevUnits || []).map((u) => u.unit_key).filter(Boolean));
  const currSet = new Set((currUnits || []).map((u) => u.unit_key).filter(Boolean));
  const appeared = [...currSet].filter((k) => !prevSet.has(k));
  const disappeared = [...prevSet].filter((k) => !currSet.has(k));
  return { appeared, disappeared };
}

function looksLikeMoney(x) {
  const s = String(x ?? "");
  return /\$?\s*\d{3,7}(?:[,.]\d{2})?/.test(s);
}

function parseMoneyToNumber(x) {
  // "$2,215" -> 2215
  return toNumberMaybe(x);
}

function isLikelyUnitsPayload(obj) {
  // Heuristics: RealPage payloads vary by site; we accept if it contains
  // an array of objects where many have unit-ish fields.
  try {
    const s = JSON.stringify(obj);
    if (s.length < 500) return false;

    // common words seen in inventory payloads
    const keywords = ["unit", "available", "rent", "price", "floorplan", "move", "sqft"];
    const hit = keywords.filter((k) => s.toLowerCase().includes(k)).length;
    return hit >= 3;
  } catch {
    return false;
  }
}

function findAnyArray(obj) {
  // Walk object and return arrays that look like "unit rows"
  const results = [];

  const seen = new Set();
  const stack = [{ path: "$", val: obj }];
  while (stack.length) {
    const { path, val } = stack.pop();

    if (val && typeof val === "object") {
      if (seen.has(val)) continue;
      seen.add(val);

      if (Array.isArray(val)) {
        if (val.length && typeof val[0] === "object") results.push({ path, arr: val });
      } else {
        for (const [k, v] of Object.entries(val)) {
          stack.push({ path: `${path}.${k}`, val: v });
        }
      }
    }
  }
  return results;
}

function extractUnitsFromUnknownJson(json, pageUrl) {
  // We don’t assume exact schema. We:
  // 1) find object arrays
  // 2) look for fields that resemble unit number, rent, available date
  // 3) normalize to our unit schema
  const arrays = findAnyArray(json);

  const units = [];
  const seen = new Set();

  for (const { path, arr } of arrays) {
    // score each array by how many items look like unit rows
    let score = 0;
    for (const item of arr.slice(0, 30)) {
      const keys = Object.keys(item || {}).map((k) => k.toLowerCase());
      const hasUnitKey = keys.some((k) => k.includes("unit"));
      const hasRent = keys.some((k) => k.includes("rent") || k.includes("price"));
      const hasAvail = keys.some((k) => k.includes("avail") || k.includes("move"));
      if (hasUnitKey) score += 2;
      if (hasRent) score += 1;
      if (hasAvail) score += 1;
    }
    if (score < 25) continue; // filter out irrelevant arrays

    for (const item of arr) {
      if (!item || typeof item !== "object") continue;

      // candidate fields
      const entries = Object.entries(item);

      const unitField =
        entries.find(([k]) => /unit(number|no|_no|_number)?/i.test(k)) ||
        entries.find(([k]) => /unit/i.test(k)) ||
        entries.find(([k]) => /apartment/i.test(k)) ||
        null;

      const rentField =
        entries.find(([k]) => /(rent|marketRent|price|amount)/i.test(k)) ||
        null;

      const availField =
        entries.find(([k]) => /(available|availableon|moveindate|availableDate|availDate)/i.test(k)) ||
        null;

      const unit_number = normalizeUnitNumber(unitField ? unitField[1] : null);
      if (!unit_number) continue;

      const unit_key = normalizeUnitKeyFromNumber(unit_number);
      if (!unit_key || seen.has(unit_key)) continue;
      seen.add(unit_key);

      const priceRaw = rentField ? rentField[1] : null;
      let price = null;
      if (priceRaw != null) {
        if (typeof priceRaw === "number") price = priceRaw;
        else if (looksLikeMoney(priceRaw)) {
          const m = String(priceRaw).match(/\$?\s*[\d,]{3,7}(?:\.\d{2})?/);
          if (m) price = parseMoneyToNumber(m[0]);
        }
      }

      const available_on = availField ? String(availField[1] ?? "").trim() : null;

      units.push({
        unit_key,
        unit_id: unit_key,
        unit_number,
        available_on: available_on || null,
        price,
        floor_plan_id: null,
        meta: {
          source: "RealPage",
          page_url: pageUrl,
          json_path: path,
          raw: item,
        },
      });
    }

    // If we got a good set, don’t keep merging from weaker arrays (reduces duplicates/noise)
    if (units.length >= 5) break;
  }

  return units;
}

// ================== SNAPSHOT + EVENTS ==================
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

// ================== REALPAGE SNIFF RUN ==================
async function scrapeRealPageUnits() {
  const browser = await chromium.launch({ headless: REALPAGE_HEADLESS });

  const context = await browser.newContext({
    userAgent:
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    viewport: { width: 1280, height: 800 },
    locale: "en-US",
  });

  const page = await context.newPage();

  const captured = [];
  let saved = 0;

  page.on("response", async (res) => {
    try {
      const ct = (res.headers()["content-type"] || "").toLowerCase();
      if (!ct.includes("application/json") && !ct.includes("text/json")) return;

      const url = res.url();
      const body = await res.text();
      if (!body || body.length < 300) return;

      let json = null;
      try {
        json = JSON.parse(body);
      } catch {
        return;
      }

      if (!isLikelyUnitsPayload(json)) return;

      captured.push({ url, json });

      if (REALPAGE_SAVE_JSON) {
        saved += 1;
        if (saved <= 12) {
          const safe = url.replace(/[^a-z0-9]+/gi, "_").slice(0, 140);
          const file = `realpage_json_${String(saved).padStart(2, "0")}_${safe}.txt`;
          safeWriteFile(file, `URL: ${url}\n\n${body}`);
          console.log("Saved JSON:", file);
        }
      }
    } catch {}
  });

  try {
    console.log("Opening:", REALPAGE_URL);
    const resp = await page.goto(REALPAGE_URL, {
      waitUntil: "networkidle",
      timeout: 60000,
    });
    console.log("HTTP status:", resp ? resp.status() : null);

    // Let app hydrate + fire inventory calls
    await page.waitForTimeout(4500);

    // Nudge SPA routes / lazy loads
    for (let i = 0; i < 3; i++) {
      await page.mouse.wheel(0, 1400).catch(() => {});
      await page.waitForTimeout(800);
    }

    // Try clicking common UI labels if present
    const clickTargets = ["Floor Plans", "Availability", "Available", "Units", "View All"];
    for (const t of clickTargets) {
      await page.getByRole("link", { name: t }).first().click({ timeout: 1200 }).catch(() => {});
      await page.getByRole("button", { name: t }).first().click({ timeout: 1200 }).catch(() => {});
      await page.waitForTimeout(600);
    }

    await page.waitForTimeout(2500);

    // Debug screenshot (optional)
    try {
      await page.screenshot({ path: "realpage_debug.png", fullPage: true });
      console.log("Saved screenshot: realpage_debug.png");
    } catch {}

    // Parse captured JSON for units
    let allUnits = [];
    for (const c of captured) {
      const u = extractUnitsFromUnknownJson(c.json, REALPAGE_URL);
      if (u.length) allUnits.push(...u);
    }

    // Dedup by unit_key
    const seen = new Set();
    const deduped = [];
    for (const u of allUnits) {
      if (!u?.unit_key) continue;
      if (seen.has(u.unit_key)) continue;
      seen.add(u.unit_key);
      deduped.push(u);
    }

    console.log("Captured JSON payloads:", captured.length);
    return deduped;
  } finally {
    await page.close().catch(() => {});
    await context.close().catch(() => {});
    await browser.close().catch(() => {});
  }
}

// ================== MAIN ==================
async function main() {
  const snapshotDate = SNAPSHOT_DATE || todayYYYYMMDD();

  console.log("Fetching RealPage data...");
  console.log("REALPAGE_URL:", REALPAGE_URL);

  const unitsToday = await scrapeRealPageUnits();
  console.log("Units extracted:", unitsToday.length);

  if (!unitsToday.length) {
    console.log("Extracted 0 units. Next step: inspect realpage_json_*.txt to identify the correct inventory endpoint/schema.");
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

  console.log("✅ Snapshot upserted:", row);
  console.log("✅ Events written:", events.length);
}

main()
  .then(() => setTimeout(() => process.exit(0), 200))
  .catch((err) => {
    console.error("❌ Failed:", err?.message || err);
    setTimeout(() => process.exit(1), 200);
  });
