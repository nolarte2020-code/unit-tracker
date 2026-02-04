/**
 * RealPage snapshot v1 (minimal)
 * Goal: fetch the RealPage page, discover the JSON endpoint (or embedded data),
 * and print a normalized list of available units.
 *
 * This is intentionally v1: it focuses on "get units somehow" first.
 */
const fs = require("fs");

function getEnvValue(filePath, key) {
  const text = fs.readFileSync(filePath, "utf8");
  const line = text.split(/\r?\n/).find((l) => l.trim().startsWith(`${key}=`));
  if (!line) return null;
  return line.split("=").slice(1).join("=").trim().replace(/^"|"$/g, "");
}

async function fetchText(url, headers = {}) {
  const res = await fetch(url, { headers, redirect: "follow" });
  const text = await res.text();
  return { status: res.status, url: res.url, text, headers: res.headers };
}

function extractK(realpageUrl) {
  // https://.../#k=22446
  const m = realpageUrl.match(/[#?]k=(\d+)/i);
  return m ? m[1] : null;
}

(async () => {
  const envFile = process.argv[2];
  if (!envFile) {
    console.error("Usage: node snapshot_realpage_v1.js <envFile>");
    process.exit(1);
  }

  const realpageUrl = getEnvValue(envFile, "REALPAGE_URL");
  if (!realpageUrl) {
    console.error("Missing REALPAGE_URL in env");
    process.exit(1);
  }

  const k = extractK(realpageUrl);
  if (!k) {
    console.error("Could not extract #k=##### from REALPAGE_URL");
    process.exit(1);
  }

  // RealPage sometimes blocks non-browsery clients; use simple browser-like headers
  const headers = {
    "User-Agent":
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
  };

  console.log("▶ Fetching RealPage landing:", realpageUrl);
  const page = await fetchText(realpageUrl, headers);
  console.log("Status:", page.status);

  // If we get HTML that contains obvious unit JSON endpoints, grab them.
  // Common clues: "availableUnits", "units", "api", ".json", "inventory"
  const html = page.text;

  // Quick reality check
  if (html.includes("Just a moment") || html.includes("challenge-platform") || html.includes("_cf_chl_opt")) {
    console.error("❌ Blocked by Cloudflare/anti-bot on RealPage landing.");
    process.exit(2);
  }

  // Try to find an API URL inside the HTML/JS references
  const apiCandidates = new Set();

  // Look for absolute URLs that smell like API
  const urlRegex = /https?:\/\/[^\s"'<>]+/g;
  const urls = html.match(urlRegex) || [];
  for (const u of urls) {
    if (/api|units|inventory|availability|floorplan|json/i.test(u)) apiCandidates.add(u);
  }

  // Also look for relative API-like paths
  const relRegex = /["'](\/[^"']+?)["']/g;
  let m;
  while ((m = relRegex.exec(html))) {
    const p = m[1];
    if (/api|units|inventory|availability|floorplan|json/i.test(p)) apiCandidates.add(p);
  }

  console.log("Found API-ish candidates:", apiCandidates.size);

  // Print candidates so we can pick the right one based on your console output
  // (v1 goal: discover endpoint)
  let i = 0;
  for (const c of apiCandidates) {
    i++;
    if (i <= 30) console.log("  -", c);
  }

  console.log("\n✅ v1 complete: endpoint discovery output above.");
})();
