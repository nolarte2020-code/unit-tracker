// diff_snapshots_to_unit_events_v2.js
// Universal snapshot differ:
// - Finds last 2 snapshots per property
// - Computes appeared/disappeared
// - Writes unit_events for latest snapshot_date
// Works for BOTH RentCafe + SightMap snapshots.
//
// Modes:
// 1) Single property: set PROPERTY_ID in env
// 2) Batch by platform: set PLATFORM in env (e.g. "sightmap" or "rentcafe")
// 3) All properties with snapshots: default if neither is set Created by OV Agency

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const PROPERTY_ID = (process.env.PROPERTY_ID || "").trim(); // optional
const PLATFORM = (process.env.PLATFORM || "").trim(); // optional: "sightmap" | "rentcafe" | etc
const SNAPSHOT_DATE = (process.env.SNAPSHOT_DATE || "").trim(); // optional YYYY-MM-DD
const EVENT_SOURCE = (process.env.EVENT_SOURCE || "snapshot").trim(); // e.g. "snapshot"

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function normalizeUnitKey(u) {
  // RentCafe: unit_id "unit:217"
  // SightMap: unit_id "945458", unit_number "01077"
  const unitId = u?.unit_id != null ? String(u.unit_id) : "";
  const unitNumber = u?.unit_number != null ? String(u.unit_number).trim() : "";

  if (unitId.startsWith("unit:")) return unitId;
  if (unitNumber) return `unit:${unitNumber}`;
  if (unitId) return `id:${unitId}`;
  return null;
}

function unitNumberFromKey(unitKey) {
  if (!unitKey) return null;
  const s = String(unitKey);
  if (s.startsWith("unit:")) return s.slice(5) || null;
  return null;
}

function buildMap(units) {
  // Map(unit_key -> { unit_number, raw })
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
  // Mode 1: single property
  if (PROPERTY_ID) return [{ id: PROPERTY_ID }];

  // Mode 2: by platform
  if (PLATFORM) {
    const { data, error } = await supabase
      .from("properties")
      .select("id")
      .eq("platform", PLATFORM)
      .limit(10000);

    if (error) throw error;

    return (data || []).map((r) => ({ id: r.id }));
  }

  // Mode 3: all properties that have snapshots
  // NOTE: this used to pull a bunch of duplicates; we fix it by selecting distinct in JS,
  // but we also request only property_id field to keep it light.
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

  if (SNAPSHOT_DATE) q = q.lte("snapshot_date", SNAPSHOT_DATE);

  const { data, error } = await q
    .order("snapshot_date", { ascending: false })
    .limit(2);

  if (error) throw error;
  return data || [];
}

async function writeEvents({
  propertyId,
  eventDate,
  appearedKeys,
  disappearedKeys,
  currMap,
  prevMap,
}) {
  // Delete existing events for same property/date/source to make it re-runnable
  const { error: delErr } = await supabase
    .from("unit_events")
    .delete()
    .eq("property_id", propertyId)
    .eq("event_date", eventDate)
    .eq("source", EVENT_SOURCE);

  if (delErr) throw delErr;

  const rows = [];

  // Appeared units: should exist in currMap
  for (const k of appearedKeys) {
    const unit_number =
      currMap.get(k)?.unit_number || unitNumberFromKey(k) || null;

    rows.push({
      property_id: propertyId,
      event_date: eventDate,
      unit_key: k,
      unit_number,
      event_type: "appeared",
      source: EVENT_SOURCE,
    });
  }

  // Disappeared units: unit_number should come from prevMap (best), else parse from key
  for (const k of disappearedKeys) {
    const unit_number =
      prevMap.get(k)?.unit_number || unitNumberFromKey(k) || null;

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
  console.log("Diff snapshots -> unit_events (v2)");
  console.log("EVENT_SOURCE:", EVENT_SOURCE);
  if (PROPERTY_ID) console.log("MODE: single property", PROPERTY_ID);
  else if (PLATFORM) console.log("MODE: platform batch", PLATFORM);
  else console.log("MODE: all properties with snapshots");
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
        prevMap,
      });

      totalInserted += inserted;

      console.log(
        `- ${p.id} @ ${latestDate}: +${appeared.length} appeared, -${disappeared.length} disappeared (inserted ${inserted})`
      );

      await sleep(120);
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
