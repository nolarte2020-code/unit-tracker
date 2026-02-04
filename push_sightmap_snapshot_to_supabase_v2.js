// push_sightmap_snapshot_to_supabase_v2.js
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

// Env vars per property
const SIGHTMAP_PROPERTY_ID = process.env.SIGHTMAP_PROPERTY_ID; // uuid from Supabase properties table
const SIGHTMAP_ASSET = process.env.SIGHTMAP_ASSET; // e.g. rx1p83kkwd6
const SIGHTMAP_LANDING_PAGE_ID = process.env.SIGHTMAP_LANDING_PAGE_ID; // e.g. 6641

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in .env");
  process.exit(1);
}
if (!SIGHTMAP_PROPERTY_ID || !SIGHTMAP_ASSET || !SIGHTMAP_LANDING_PAGE_ID) {
  console.error(
    "Missing SIGHTMAP_PROPERTY_ID or SIGHTMAP_ASSET or SIGHTMAP_LANDING_PAGE_ID in .env"
  );
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

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

async function fetchSightmapLandingPage(assetCode, landingPageId) {
  const url = `https://sightmap.com/app/api/v1/${assetCode}/landing-pages/${landingPageId}`;

  const attempts = 3;
  let lastErr;

  for (let i = 1; i <= attempts; i++) {
    try {
      const res = await fetch(url, { headers: { Accept: "application/json" } });
      if (!res.ok) throw new Error(`Sightmap HTTP ${res.status} ${res.statusText}`);
      return await res.json();
    } catch (err) {
      lastErr = err;
      await sleep(600 * i);
    }
  }

  throw lastErr;
}

// Normalize keys so all platforms match
function normalizeUnitNumber(n) {
  const s = String(n ?? "").trim();
  return s ? s : null;
}
function normalizeUnitKeyFromUnitNumber(unitNumber) {
  const n = normalizeUnitNumber(unitNumber);
  return n ? `unit:${n}` : null;
}

function extractUnits(payload) {
  const units = payload?.data?.units;
  if (!Array.isArray(units)) return [];

  return units
    .map((u) => {
      const unitNumber = normalizeUnitNumber(u.unit_number);
      const unitKey = normalizeUnitKeyFromUnitNumber(unitNumber);

      return {
        unit_id: String(u.id),
        unit_number: unitNumber,
        unit_key: unitKey, // NEW (not required in snapshots, but helpful)
        available_on: u.available_on ?? null, // "YYYY-MM-DD"
        price: typeof u.price === "number" ? u.price : null,
        floor_plan_id: u.floor_plan_id ? String(u.floor_plan_id) : null,

        // Optional extras
        building: u.building ?? null,
        area: typeof u.area === "number" ? u.area : null,
      };
    })
    // Keep only units that have a unit_number (SightMap always should)
    .filter((u) => u.unit_number);
}

function diffUnits(prevUnits, currUnits) {
  const prevSet = new Set((prevUnits || []).map((u) => u?.unit_key).filter(Boolean));
  const currSet = new Set((currUnits || []).map((u) => u?.unit_key).filter(Boolean));

  const appeared = [...currSet].filter((k) => !prevSet.has(k));
  const disappeared = [...prevSet].filter((k) => !currSet.has(k));

  return { appeared, disappeared };
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

async function loadLatestSnapshotUnits({ propertyId, snapshotDate }) {
  const { data, error } = await supabase
    .from("unit_snapshots")
    .select("snapshot_date, units_json")
    .eq("property_id", propertyId)
    .lte("snapshot_date", snapshotDate)
    .order("snapshot_date", { ascending: false })
    .limit(1);

  if (error) throw error;
  return data?.[0]?.units_json || [];
}

async function deleteEventsForDay({ propertyId, snapshotDate, source }) {
  const { error } = await supabase
    .from("unit_events")
    .delete()
    .eq("property_id", propertyId)
    .eq("event_date", snapshotDate)
    .eq("source", source);

  if (error) throw error;
}

async function insertEvents(events) {
  if (!events.length) return;
  const { error } = await supabase.from("unit_events").insert(events);
  if (error) throw error;
}

async function main() {
  const source = "sightmap";

  console.log("Fetching SightMap data...");

  const snapshotDate = process.env.SNAPSHOT_DATE || todayYYYYMMDD();

  // 1) Get current payload
  const payload = await fetchSightmapLandingPage(SIGHTMAP_ASSET, SIGHTMAP_LANDING_PAGE_ID);
  const unitsToday = extractUnits(payload);

  console.log(`Units extracted: ${unitsToday.length}`);
  if (unitsToday.length === 0) {
    console.log("⚠️ Extracted 0 units — check asset/landing_page_id.");
    process.exit(2);
  }

  // 2) Load latest previous snapshot units (<= today)
  const prevUnits = await loadLatestSnapshotUnits({
    propertyId: SIGHTMAP_PROPERTY_ID,
    snapshotDate,
  });

  // 3) Compute diff using unit_key
  const { appeared, disappeared } = diffUnits(prevUnits, unitsToday);

  // 4) Upsert today's snapshot
  console.log("Pushing snapshot to Supabase...");
  const row = await upsertSnapshot({
    propertyId: SIGHTMAP_PROPERTY_ID,
    snapshotDate,
    units: unitsToday,
  });
  console.log("✅ Snapshot upserted:", row);

  // 5) Replace today's events for this property/source
  await deleteEventsForDay({
    propertyId: SIGHTMAP_PROPERTY_ID,
    snapshotDate,
    source,
  });

  // 6) Build a lookup so we always have unit_number
  const keyToNum = new Map();
  for (const u of unitsToday) {
    if (u.unit_key && u.unit_number) keyToNum.set(u.unit_key, u.unit_number);
  }

  const events = [
    ...appeared.map((unit_key) => ({
      property_id: SIGHTMAP_PROPERTY_ID,
      event_date: snapshotDate,
      unit_key,
      unit_number: keyToNum.get(unit_key) || (unit_key.startsWith("unit:") ? unit_key.slice(5) : null),
      event_type: "appeared",
      source,
    })),
    ...disappeared.map((unit_key) => ({
      property_id: SIGHTMAP_PROPERTY_ID,
      event_date: snapshotDate,
      unit_key,
      unit_number: keyToNum.get(unit_key) || (unit_key.startsWith("unit:") ? unit_key.slice(5) : null),
      event_type: "disappeared",
      source,
    })),
  ];

  await insertEvents(events);

  console.log(
    `✅ Events written for ${snapshotDate}: +${appeared.length} appeared, -${disappeared.length} disappeared`
  );
}

// Clean exit wrapper (prevents Windows async handle crash)
main()
  .then(() => setTimeout(() => process.exit(0), 200))
  .catch((err) => {
    console.error("❌ Failed:", err?.message || err);
    setTimeout(() => process.exit(1), 200);
  });
