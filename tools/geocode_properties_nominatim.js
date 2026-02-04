/**
 * Batch geocode properties.address/city/zip into properties.lat/lng using OpenStreetMap Nominatim.
 *
 * ✅ Uses Supabase service role key (server-side).
 * ✅ Rate limited (default 1 request/sec).
 * ✅ Writes geocode metadata into DB for auditability.
 * ✅ Skips rows that already have lat/lng unless --force
 *
 * Usage:
 *   node tools/geocode_properties_nominatim.js --limit=50
 *   node tools/geocode_properties_nominatim.js --only-missing
 *   node tools/geocode_properties_nominatim.js --force
 *   node tools/geocode_properties_nominatim.js --rep=<rep_id>
 *
 * Notes:
 * - Nominatim policy expects polite use. Keep rate <= 1/sec.
 * - For best results, include full street address + city + zip.
 */

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import pLimit from "p-limit";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in environment.");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

// ----- CLI args -----
const args = process.argv.slice(2);
const getArg = (name, def = null) => {
  const hit = args.find((a) => a.startsWith(`--${name}=`));
  if (!hit) return def;
  return hit.split("=").slice(1).join("=");
};

const hasFlag = (name) => args.includes(`--${name}`);

const LIMIT = Number(getArg("limit", "500")); // how many rows to process max
const ONLY_MISSING = hasFlag("only-missing"); // only rows with no lat/lng
const FORCE = hasFlag("force"); // overwrite existing lat/lng
const REP_ID = getArg("rep", null); // only properties for a rep_id
const RATE_MS = Number(getArg("rate_ms", "1100")); // delay between calls ~1/sec

// Concurrency: Keep 1 to respect rate limits. We still use p-limit for clarity.
const limit = pLimit(1);

function buildQueryString(p) {
  const a = (p.address || "").trim();
  const c = (p.city || "").trim();
  const z = (p.zip || "").trim();

  // If address is empty, fallback to name + city + zip (less accurate)
  const parts = [];
  if (a) parts.push(a);
  else if (p.name) parts.push(p.name);

  if (c) parts.push(c);
  if (z) parts.push(z);

  return parts.filter(Boolean).join(", ");
}

async function sleep(ms) {
  await new Promise((r) => setTimeout(r, ms));
}

async function nominatimSearch(q) {
  // Nominatim requires a valid User-Agent. Use something identifiable.
  const url = new URL("https://nominatim.openstreetmap.org/search");
  url.searchParams.set("q", q);
  url.searchParams.set("format", "json");
  url.searchParams.set("addressdetails", "1");
  url.searchParams.set("limit", "1");

  const res = await fetch(url.toString(), {
    headers: {
      "User-Agent": "unit-tracker-geocoder/1.0 (contact: you@example.com)",
      "Accept-Language": "en-US,en;q=0.9",
    },
  });

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`Nominatim HTTP ${res.status}: ${text.slice(0, 200)}`);
  }
  const json = await res.json();
  return Array.isArray(json) ? json[0] : null;
}

function confidenceFromResult(result) {
  // Very rough: if postcode matched and road present => "high"
  try {
    const addr = result?.address || {};
    const hasRoad = !!(addr.road || addr.pedestrian || addr.path);
    const hasPostcode = !!addr.postcode;
    const hasCity = !!(addr.city || addr.town || addr.village || addr.suburb);
    if (hasRoad && hasPostcode && hasCity) return "high";
    if ((hasRoad && hasCity) || (hasCity && hasPostcode)) return "medium";
    return "low";
  } catch {
    return "low";
  }
}

async function fetchPropertiesToGeocode() {
  // We’ll pull in chunks if needed, but for now grab up to LIMIT.
  let q = supabase
    .from("properties")
    .select("id, name, address, city, zip, rep_id, lat, lng")
    .order("created_at", { ascending: false })
    .limit(LIMIT);

  if (REP_ID) q = q.eq("rep_id", REP_ID);

  // ONLY_MISSING affects client-side filter since null vs 0 and string values vary;
  // We'll filter in JS for safety.
  const { data, error } = await q;
  if (error) throw error;

  let rows = data || [];
  if (ONLY_MISSING) {
    rows = rows.filter((p) => p.lat == null || p.lng == null);
  }
  if (!FORCE) {
    rows = rows.filter((p) => p.lat == null || p.lng == null);
  }
  return rows;
}

async function updateProperty(id, patch) {
  const { error } = await supabase.from("properties").update(patch).eq("id", id);
  if (error) throw error;
}

async function run() {
  console.log("----- Unit Tracker Geocoder (Nominatim) -----");
  console.log("LIMIT:", LIMIT);
  console.log("ONLY_MISSING:", ONLY_MISSING);
  console.log("FORCE:", FORCE);
  console.log("REP_ID:", REP_ID || "(all)");
  console.log("RATE_MS:", RATE_MS);

  const props = await fetchPropertiesToGeocode();
  console.log(`Found ${props.length} properties to geocode.`);

  if (props.length === 0) {
    console.log("Nothing to do.");
    return;
  }

  let ok = 0;
  let fail = 0;
  let skipped = 0;

  for (const p of props) {
    await limit(async () => {
      const query = buildQueryString(p);
      if (!query) {
        skipped++;
        console.log(`SKIP ${p.id}: no address/city/zip/name`);
        return;
      }

      try {
        // be polite
        await sleep(RATE_MS);

        const result = await nominatimSearch(query);

        if (!result || !result.lat || !result.lon) {
          fail++;
          console.log(`FAIL ${p.name || p.id}: no result for "${query}"`);
          await updateProperty(p.id, {
            geocoded_at: new Date().toISOString(),
            geocode_source: "nominatim",
            geocode_confidence: "none",
            geocode_query: query,
          });
          return;
        }

        const lat = Number(result.lat);
        const lng = Number(result.lon);
        const confidence = confidenceFromResult(result);

        await updateProperty(p.id, {
          lat,
          lng,
          geocoded_at: new Date().toISOString(),
          geocode_source: "nominatim",
          geocode_confidence: confidence,
          geocode_query: query,
        });

        ok++;
        console.log(`OK   ${p.name || p.id}: (${lat.toFixed(6)}, ${lng.toFixed(6)}) [${confidence}]`);
      } catch (e) {
        fail++;
        console.log(`ERR  ${p.name || p.id}: ${e.message}`);
        try {
          await updateProperty(p.id, {
            geocoded_at: new Date().toISOString(),
            geocode_source: "nominatim",
            geocode_confidence: "error",
            geocode_query: buildQueryString(p),
          });
        } catch {
          // ignore update failures
        }
      }
    });
  }

  console.log("----- DONE -----");
  console.log("OK:", ok);
  console.log("FAIL:", fail);
  console.log("SKIPPED:", skipped);
  console.log("Tip: In Map tab, anything missing coords will show in the Missing list.");
}

run().catch((e) => {
  console.error("Fatal:", e);
  process.exit(1);
});
