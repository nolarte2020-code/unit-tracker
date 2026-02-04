import fs from "fs";

const URL = "https://sightmap.com/app/api/v1/rkwnoxlevd2/landing-pages/26161";

function stamp() {
  const d = new Date();
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  const hh = String(d.getHours()).padStart(2, "0");
  const mi = String(d.getMinutes()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}_${hh}${mi}`;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function fetchWithRetry(url, { retries = 5, timeoutMs = 30000 } = {}) {
  let lastErr;

  for (let attempt = 1; attempt <= retries; attempt++) {
    const controller = new AbortController();
    const t = setTimeout(() => controller.abort(), timeoutMs);

    try {
      const res = await fetch(url, {
        headers: { Accept: "application/json" },
        signal: controller.signal,
      });
      clearTimeout(t);

      if (!res.ok) throw new Error(`HTTP ${res.status} ${res.statusText}`);
      return await res.json();
    } catch (e) {
      clearTimeout(t);
      lastErr = e;
      console.log(`Fetch attempt ${attempt}/${retries} failed: ${e?.message || e}`);

      if (attempt < retries) {
        // backoff: 2s, 4s, 6s, 8s...
        await sleep(2000 * attempt);
      }
    }
  }

  throw lastErr;
}

async function main() {
  const json = await fetchWithRetry(URL, { retries: 5, timeoutMs: 30000 });
  const units = json?.data?.units ?? [];

  const simplified = units.map((u) => ({
    unit_id: u.id,
    unit_number: u.unit_number,
    available_on: u.available_on,
    price: u.price,
    floor_plan_id: u.floor_plan_id,
  }));

  simplified.sort((a, b) => Number(a.unit_number) - Number(b.unit_number));

  if (!fs.existsSync("snapshots")) fs.mkdirSync("snapshots");
  const file = `snapshots/sofi_${stamp()}.json`;
  fs.writeFileSync(file, JSON.stringify(simplified, null, 2));

  console.log(`Saved snapshot: ${file}`);
  console.log(`Total units: ${simplified.length}`);
}

main().catch((e) => {
  console.error("Failed:", e);
  process.exit(1);
});
