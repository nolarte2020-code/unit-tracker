// diff_and_push_changes.js
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in .env");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

function yyyyMmDd(d) {
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

function getSnapshotDateFromEnvOrToday() {
  // if you set SNAPSHOT_DATE, we use that as "current"
  if (process.env.SNAPSHOT_DATE) return process.env.SNAPSHOT_DATE;
  return yyyyMmDd(new Date());
}

function previousDay(dateStr) {
  const d = new Date(dateStr + "T00:00:00");
  d.setDate(d.getDate() - 1);
  return yyyyMmDd(d);
}

function keyUnit(u) {
  // unit_id should be stable
  return String(u.unit_id);
}

function normalizeUnit(u) {
  return {
    unit_id: u.unit_id ?? null,
    unit_number: u.unit_number ?? null,
    available_on: u.available_on ?? null,
    price: typeof u.price === "number" ? u.price : (u.price ? Number(u.price) : null),
    floor_plan_id: u.floor_plan_id ?? null,
  };
}

function unitsToMap(unitsJson) {
  const map = new Map();
  for (const raw of unitsJson || []) {
    const u = normalizeUnit(raw);
    if (!u.unit_id) continue;
    map.set(keyUnit(u), u);
  }
  return map;
}

function diffUnits(prevUnits, currUnits) {
  const prevMap = unitsToMap(prevUnits);
  const currMap = unitsToMap(currUnits);

  const changes = [];

  // NEW + CHANGED
  for (const [id, curr] of currMap.entries()) {
    const prev = prevMap.get(id);
    if (!prev) {
      changes.push({
        change_type: "NEW",
        unit_id: id,
        unit_number: curr.unit_number,
        before: null,
        after: curr,
      });
      continue;
    }

    // Compare important fields
    const fields = ["available_on", "price", "floor_plan_id", "unit_number"];
    const changed = fields.some((f) => (prev?.[f] ?? null) !== (curr?.[f] ?? null));

    if (changed) {
      changes.push({
        change_type: "CHANGED",
        unit_id: id,
        unit_number: curr.unit_number ?? prev.unit_number ?? null,
        before: prev,
        after: curr,
      });
    }
  }

  // REMOVED
  for (const [id, prev] of prevMap.entries()) {
    if (!currMap.has(id)) {
      changes.push({
        change_type: "REMOVED",
        unit_id: id,
        unit_number: prev.unit_number,
        before: prev,
        after: null,
      });
    }
  }

  return changes;
}

async function getSnapshot(propertyId, snapshotDate) {
  const { data, error } = await supabase
    .from("unit_snapshots")
    .select("property_id, snapshot_date, units_json")
    .eq("property_id", propertyId)
    .eq("snapshot_date", snapshotDate)
    .maybeSingle();

  if (error) throw error;
  return data; // may be null
}

async function upsertChanges(propertyId, snapshotDate, changes) {
  if (!changes.length) return { inserted: 0 };

  // Optional: clear existing diffs for that date/property then insert fresh
  // (simplest way to avoid duplicates if you run it twice)
  const del = await supabase
    .from("unit_changes")
    .delete()
    .eq("property_id", propertyId)
    .eq("snapshot_date", snapshotDate);

  if (del.error) throw del.error;

  const rows = changes.map((c) => ({
    property_id: propertyId,
    snapshot_date: snapshotDate,
    change_type: c.change_type,
    unit_id: c.unit_id,
    unit_number: c.unit_number ?? null,
    before: c.before,
    after: c.after,
  }));

  const { error } = await supabase.from("unit_changes").insert(rows);
  if (error) throw error;

  return { inserted: rows.length };
}

async function main() {
  const propertyId = process.env.SIGHTMAP_PROPERTY_ID || process.env.SOFI_PROPERTY_ID;

  if (!propertyId) {
    console.error("Missing SIGHTMAP_PROPERTY_ID (or SOFI_PROPERTY_ID) in .env");
    process.exit(1);
  }

  const currDate = getSnapshotDateFromEnvOrToday();
  const prevDate = previousDay(currDate);

  console.log("Property:", propertyId);
  console.log("Prev date:", prevDate);
  console.log("Curr date:", currDate);

  const prevSnap = await getSnapshot(propertyId, prevDate);
  const currSnap = await getSnapshot(propertyId, currDate);

  if (!currSnap) {
    console.error(`No snapshot found for ${currDate}. Run push_*_snapshot first.`);
    process.exit(1);
  }

  const prevUnits = prevSnap?.units_json || [];
  const currUnits = currSnap.units_json || [];

  console.log(`Prev units: ${prevUnits.length}`);
  console.log(`Curr units: ${currUnits.length}`);

  if (!prevSnap) {
    console.log("No previous snapshot found. (Nothing to diff yet.)");
    process.exit(0);
  }

  const changes = diffUnits(prevUnits, currUnits);

  const counts = changes.reduce(
    (acc, c) => {
      acc[c.change_type] = (acc[c.change_type] || 0) + 1;
      return acc;
    },
    {}
  );

  console.log("Changes:", counts);

  const result = await upsertChanges(propertyId, currDate, changes);
  console.log("✅ Saved changes:", result);
}

main()
  .then(() => setTimeout(() => process.exit(0), 200))
  .catch((err) => {
    console.error("❌ Failed:", err?.message || err);
    setTimeout(() => process.exit(1), 200);
  });
