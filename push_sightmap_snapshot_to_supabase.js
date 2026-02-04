// push_sightmap_snapshot_to_supabase.js
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

// New generic env vars for any SightMap property
const SIGHTMAP_PROPERTY_ID = process.env.SIGHTMAP_PROPERTY_ID; // uuid from your Supabase properties table
const SIGHTMAP_ASSET = process.env.SIGHTMAP_ASSET; // e.g. rx1p83kkwd6 OR rkwnoxlevd2 (depends on property)
const SIGHTMAP_LANDING_PAGE_ID = process.env.SIGHTMAP_LANDING_PAGE_ID; // e.g. 6641, 26161, etc

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
  // local date (good enough for daily snapshots)
  const d = new Date();
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

async function fetchSightmapLandingPage(assetCode, landingPageId) {
  // This matches what worked for Sofi and also what you pulled for Essex:
  // https://sightmap.com/app/api/v1/{asset}/landing-pages/{landingPageId}
  const url = `https://sightmap.com/app/api/v1/${assetCode}/landing-pages/${landingPageId}`;

  const attempts = 3;
  let lastErr;

  for (let i = 1; i <= attempts; i++) {
    try {
      const res = await fetch(url, {
        headers: { Accept: "application/json" },
      });

      if (!res.ok) {
        throw new Error(`Sightmap HTTP ${res.status} ${res.statusText}`);
      }

      return await res.json();
    } catch (err) {
      lastErr = err;
      // small backoff for timeouts
      await new Promise((r) => setTimeout(r, 600 * i));
    }
  }

  throw lastErr;
}

function extractUnits(payload) {
  const units = payload?.data?.units;
  if (!Array.isArray(units)) return [];

  return units.map((u) => ({
    unit_id: String(u.id),
    unit_number: u.unit_number ?? null,
    available_on: u.available_on ?? null, // "YYYY-MM-DD"
    price: typeof u.price === "number" ? u.price : null,
    floor_plan_id: u.floor_plan_id ? String(u.floor_plan_id) : null,

    // Optional extras (nice to have later)
    building: u.building ?? null,
    area: typeof u.area === "number" ? u.area : null,
  }));
}

async function upsertSnapshot({ propertyId, snapshotDate, units }) {
  // unit_snapshots columns:
  // id uuid, property_id uuid, snapshot_date date, units_json jsonb, created_at timestamptz
  //
  // IMPORTANT: Your DB should have a UNIQUE constraint on (property_id, snapshot_date)
  // so multiple properties can store the same date.
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
  console.log("Fetching SightMap data...");

  const payload = await fetchSightmapLandingPage(
    SIGHTMAP_ASSET,
    SIGHTMAP_LANDING_PAGE_ID
  );

  const units = extractUnits(payload);
  console.log(`Units extracted: ${units.length}`);

  const snapshotDate = process.env.SNAPSHOT_DATE || todayYYYYMMDD();

  console.log("Pushing snapshot to Supabase...");
  const row = await upsertSnapshot({
    propertyId: SIGHTMAP_PROPERTY_ID,
    snapshotDate,
    units,
  });

  console.log("✅ Snapshot upserted:", row);
}

// Clean exit wrapper (prevents Windows async handle crash)
main()
  .then(() => setTimeout(() => process.exit(0), 200))
  .catch((err) => {
    console.error("❌ Failed:", err?.message || err);
    setTimeout(() => process.exit(1), 200);
  });
