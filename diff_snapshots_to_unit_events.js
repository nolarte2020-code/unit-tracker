// diff_snapshots_to_unit_events.js
// Universal: reads last 2 snapshots for a property, computes appeared/disappeared,
// writes unit_events for the latest snapshot_date.
// Designed to work for BOTH RentCafe + SightMap snapshots.

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

// Optional: run for one property (recommended while testing)
const PROPERTY_ID = process.env.PROPERTY_ID || "";

// Optional: if set, diff snapshots up to this date (YYYY-MM-DD). Otherwise use latest 2 snapshots.
const SNAPSHOT_DATE = process.env.SNAPSHOT_DATE || "";

// Event source label (so you can distinguish from "scan")
const EVENT_SOURCE = process.env.EVENT_SOURCE || "snapshot";

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function normalizeUnitKey(u) {
  // u is one element of units_json from unit_snapshots
  // RentCafe: unit_id like "unit:217"
  // SightMap: unit_id like "945458"
  const unitId = u?.unit_id != null ? String(u.unit_id) : "";
  const unitNumber = u?.unit_number != null ? String(u.unit_number).trim() : "";

  if (unitId.startsWith("unit:")) return unitId;

  if (unitNumber) return `unit:${unitNumber}`;

  if (unitId) return `id:${unitId}`;

  // absolute fallback (should be rare)
  return null;
}

function unitNumberFromKey(unitKey) {
  if (!unitKey) return null;
  const s = String(unitKey);
  if (s.startsWith("unit:")) return s.slice(5) || null;
  return null;
}

function buildMap(units) {
  // Returns: Map(unit_key -> { unit_number, raw })
  const m = new Map();
  for (const u of units || []) {
    const key = normalizeUnitKey(u);
    if (!key) continue;

    const unit_number =
      (u?.unit_number != null && String(u.unit_number).trim()) ||
      unitNumberFromKey(key) ||
      null;

    m.set(key, { unit_number, raw: u });
  }
  return m;
}

function diffKeys(prevMap, currMap) {
  const prevKeys = new Set(prevMap.keys());
  const currKeys = new Set(currMap.keys());

  const appeared = [];
  const disappeared = [];

  for (const k of currKeys) if (!prevKeys.has(k)) appeared.push(k);
  for (const k of prevKeys) if (!currKeys.has(k)) disappeared.push(k);

  return { appeared, disappeared };
}

async function getPropertiesToProcess() {
  if (PROPERTY_ID) return [{ id: PROPERTY_ID }];

  // If no PROPERTY_ID provided, process ALL properties that have snapshots
  // (Safer later when you run “all properties” nightly.)
  const { data, error } = await supabase
    .from("unit_snapshots")
    .select("property_id")
    .limit(10000);

  if (error) throw error;

  const uniq = Array.from(new Set((data || []).map((r) => r.property_id)));
  return uniq.map((id) => ({ id }));
}

async function getLastTwoSnapshots(propertyId) {
  let q = supabase
    .from("unit_snapshots")
    .select("snapshot_date, units_json")
    .eq("property_id", propertyId);

  if (SNAPSHOT_DATE) {
    q = q.lte("snapshot_date", SNAPSHOT_DATE);
  }

  const { data, error } = await q
    .order("snapshot_date", { ascending: false })
    .limit(2);

  if (error) throw error;

  return data || [];
}

async function writeEvents({ propertyId, eventDate, appearedKeys, disappearedKeys, currMap }) {
  // Prevent duplicates if you re-run today: delete today’s events for this source
  const { error: delErr } = await supabase
    .from("unit_events")
    .delete()
    .eq("property_id", propertyId)
    .eq("event_date", eventDate)
    .eq("source", EVENT_SOURCE);

  if (delErr) throw delErr;

  const rows = [];

  for (const k of appearedKeys) {
    const unit_number = currMap.get(k)?.unit_number || unitNumberFromKey(k) || null;
    rows.push({
      property_id: propertyId,
      event_date: eventDate,
      unit_key: k,
      unit_number,
      event_type: "appeared",
      source: EVENT_SOURCE,
    });
  }

  for (const k of disappearedKeys) {
    // disappeared rows may not exist in currMap, so unit_number may be null — acceptable
    // (still useful for counts). If you want, we can also pull from prevMap later.
    const unit_number = unitNumberFromKey(k) || null;
    rows.push({
      property_id: propertyId,
      event_date: eventDate,
      unit_key: k,
      unit_number,
      event_type: "disappeared",
      source: EVENT_SOURCE,
    });
  }

  if (rows.length === 0) return { inserted: 0 };

  const { error: insErr } = await supabase.from("unit_events").insert(rows);
  if (insErr) throw insErr;

  return { inserted: rows.length };
}

async function main() {
  console.log("Diff snapshots -> unit_events");
  console.log("EVENT_SOURCE:", EVENT_SOURCE);
  if (PROPERTY_ID) console.log("PROPERTY_ID:", PROPERTY_ID);
  if (SNAPSHOT_DATE) console.log("SNAPSHOT_DATE <=", SNAPSHOT_DATE);

  const props = await getPropertiesToProcess();
  console.log("Properties to process:", props.length);

  let totalInserted = 0;

  for (const p of props) {
    try {
      const snaps = await getLastTwoSnapshots(p.id);

      if (snaps.length < 2) {
        console.log(`- ${p.id}: skip (need 2 snapshots)`);
        continue;
      }

      const [latest, prev] = snaps;

      const latestDate = latest.snapshot_date;
      const currUnits = latest.units_json || [];
      const prevUnits = prev.units_json || [];

      const prevMap = buildMap(prevUnits);
      const currMap = buildMap(currUnits);

      const { appeared, disappeared } = diffKeys(prevMap, currMap);

      const { inserted } = await writeEvents({
        propertyId: p.id,
        eventDate: latestDate,
        appearedKeys: appeared,
        disappearedKeys: disappeared,
        currMap,
      });

      totalInserted += inserted;

      console.log(
        `- ${p.id} @ ${latestDate}: +${appeared.length} appeared, -${disappeared.length} disappeared (inserted ${inserted})`
      );

      // small pacing to avoid hammering Supabase
      await sleep(150);
    } catch (err) {
      console.error(`❌ ${p.id}:`, err?.message || err);
    }
  }

  console.log("✅ Done. Total inserted:", totalInserted);
}

main()
  .then(() => setTimeout(() => process.exit(0), 200))
  .catch((err) => {
    console.error("❌ Failed:", err?.message || err);
    setTimeout(() => process.exit(1), 200);
  });
