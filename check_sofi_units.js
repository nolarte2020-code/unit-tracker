// check_sofi_units.js
// Fetch units from SightMap landing page API and print a clean list.

const URL = "https://sightmap.com/app/api/v1/rkwnoxlevd2/landing-pages/26161";

async function main() {
  const res = await fetch(URL, {
    headers: { Accept: "application/json" },
  });

  if (!res.ok) {
    console.error("HTTP error:", res.status, res.statusText);
    process.exit(1);
  }

  const json = await res.json();
  const units = json?.data?.units ?? [];

  const simplified = units.map((u) => ({
    unit_id: u.id,
    unit_number: u.unit_number,
    available_on: u.available_on,
    price: u.price,
    floor_plan_id: u.floor_plan_id,
  }));

  // Sort by unit_number (as number if possible)
  simplified.sort((a, b) => Number(a.unit_number) - Number(b.unit_number));

  console.log(JSON.stringify(simplified, null, 2));
  console.log(`\nTotal units: ${simplified.length}`);
}

main().catch((e) => {
  console.error("Failed:", e);
  process.exit(1);
});
