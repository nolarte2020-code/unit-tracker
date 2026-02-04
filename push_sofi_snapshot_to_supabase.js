// push_sofi_snapshot_to_supabase.js
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const SOFI_PROPERTY_ID = process.env.SOFI_PROPERTY_ID;
const SOFI_SIGHTMAP_ASSET = process.env.SOFI_SIGHTMAP_ASSET; // rkwnoxlevd2
const SOFI_SIGHTMAP_LANDING_PAGE_ID = process.env.SOFI_SIGHTMAP_LANDING_PAGE_ID; // 26161

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in .env");
  process.exit(1);
}

if (!SOFI_PROPERTY_ID || !SOFI_SIGHTMAP_ASSET || !SOFI_SIGHTMAP_LANDING_PAGE_ID) {
  console.error("Missing SOFI_PROPERTY_ID or SOFI_SIGHTMAP_ASSET or SOFI_SIGHTMAP_LANDING_PAGE_ID in .env");
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
  const url = `https://sightmap.com/app/api/v1/${assetCode}/landing-pages/${landingPageId}`;

  // basic retry for occasional timeouts
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
      // small backoff
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
  }));
}

async function upsertSnapshot({ propertyId, snapshotDate, units }) {
  // Your table columns:
  // id uuid, property_id uuid, snapshot_date date, units_json jsonb, created_at timestamptz
  //
  // Your DB currently has a unique constraint on snapshot_date (unit_snapshots_snapshot_date_key),
  // so we upsert on snapshot_date.
  //
  // NOTE: Later, you probably want a composite unique (property_id, snapshot_date) instead.
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
  console.log("Fetching Sightmap data...");

  const payload = await fetchSightmapLandingPage(
    SOFI_SIGHTMAP_ASSET,
    SOFI_SIGHTMAP_LANDING_PAGE_ID
  );

  const units = extractUnits(payload);
  console.log(`Units extracted: ${units.length}`);

  const snapshotDate = todayYYYYMMDD();

  console.log("Pushing snapshot to Supabase...");
  const row = await upsertSnapshot({
    propertyId: SOFI_PROPERTY_ID,
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
