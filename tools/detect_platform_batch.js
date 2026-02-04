// tools/detect_platform_batch.js
// Usage:
//   node .\tools\detect_platform_batch.js .\input.csv .\output.csv
//
// Env required:
//   GOOGLE_MAPS_API_KEY=...   (Places API key)
//
// Input CSV headers expected (minimum):
//   zip,assoc_code,address
// Optional input headers:
//   url
//
// Output CSV headers:
//   zip,assoc_code,address,url,platform_guess,http_status,notes

import fs from "fs";
import path from "path";

function usageExit() {
  console.log(
    "Usage: node .\\tools\\detect_platform_batch.js .\\input.csv .\\output.csv"
  );
  process.exit(1);
}

const inputPath = process.argv[2];
const outputPath = process.argv[3];
if (!inputPath || !outputPath) usageExit();

const apiKey = process.env.GOOGLE_MAPS_API_KEY || process.env.GOOGLE_API_KEY;
if (!apiKey) {
  console.error(
    "Missing GOOGLE_MAPS_API_KEY (or GOOGLE_API_KEY). Add it to your environment or .env."
  );
  process.exit(1);
}

function readText(file) {
  return fs.readFileSync(file, "utf8");
}

function writeText(file, content) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, content, "utf8");
}

// Simple CSV parse (handles quoted fields, commas inside quotes)
function parseCSV(csvText) {
  const lines = csvText.split(/\r?\n/).filter((l) => l.trim().length > 0);
  if (lines.length === 0) return { headers: [], rows: [] };

  const headers = splitCsvLine(lines[0]).map((h) => h.trim());
  const rows = [];

  for (let i = 1; i < lines.length; i++) {
    const cols = splitCsvLine(lines[i]);
    const row = {};
    for (let j = 0; j < headers.length; j++) {
      row[headers[j]] = (cols[j] ?? "").trim();
    }
    rows.push(row);
  }
  return { headers, rows };
}

function splitCsvLine(line) {
  const out = [];
  let cur = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];

    if (ch === '"' && (i === 0 || line[i - 1] !== "\\")) {
      // handle double-quote escaping ("")
      if (inQuotes && line[i + 1] === '"') {
        cur += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (ch === "," && !inQuotes) {
      out.push(cur);
      cur = "";
      continue;
    }

    cur += ch;
  }
  out.push(cur);
  return out;
}

function toCSV(headers, rows) {
  const esc = (v) => {
    const s = String(v ?? "");
    if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
    return s;
  };

  const lines = [];
  lines.push(headers.map(esc).join(","));
  for (const r of rows) {
    lines.push(headers.map((h) => esc(r[h] ?? "")).join(","));
  }
  return lines.join("\n") + "\n";
}

async function fetchWithTimeout(url, opts = {}, timeoutMs = 20000) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(url, { ...opts, signal: ctrl.signal });
    return res;
  } finally {
    clearTimeout(t);
  }
}

// ---------- Google Places helpers (New Places API) ----------

async function placesSearchText(apiKey, textQuery) {
  const res = await fetchWithTimeout(
    "https://places.googleapis.com/v1/places:searchText",
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": apiKey,
        "X-Goog-FieldMask": "places.id,places.displayName,places.formattedAddress",
      },
      body: JSON.stringify({ textQuery }),
    }
  );

  if (!res.ok) {
    const t = await res.text();
    throw new Error(`Places searchText failed: HTTP ${res.status} | ${t}`);
  }

  const data = await res.json();
  return data.places || [];
}

async function placesGetDetails(apiKey, placeId) {
  const res = await fetchWithTimeout(`https://places.googleapis.com/v1/places/${placeId}`, {
    headers: {
      "X-Goog-Api-Key": apiKey,
      "X-Goog-FieldMask": "id,displayName,formattedAddress,websiteUri,googleMapsUri",
    },
  });

  if (!res.ok) {
    const t = await res.text();
    throw new Error(`Place details failed: HTTP ${res.status} | ${t}`);
  }

  return await res.json();
}

async function findWebsiteForAddress(apiKey, address) {
  const queries = [
    address,
    `${address} apartments`,
    `${address} apartment homes`,
    `${address} leasing office`,
    `${address} floor plans`,
    `${address} securecafe`,
    `${address} rentcafe`,
    `${address} realpage`,
  ];

  for (const q of queries) {
    let places = [];
    try {
      places = await placesSearchText(apiKey, q);
    } catch (err) {
      // propagate the first Places failure (403/etc) with clear message
      throw err;
    }

    for (const p of places.slice(0, 6)) {
      const details = await placesGetDetails(apiKey, p.id);
      if (details.websiteUri) {
        return {
          website: details.websiteUri,
          name: details.displayName?.text || "",
          formattedAddress: details.formattedAddress || "",
          maps: details.googleMapsUri || "",
          queryUsed: q,
        };
      }
    }
  }

  return null;
}

// ---------- Platform detection from URL/HTML ----------

function guessPlatformFromUrl(url) {
  const u = (url || "").toLowerCase();

  if (u.includes("securecafe.com") || u.includes("rentcafe.com")) return "RentCafe/SecureCafe";
  if (u.includes("sightmap.com")) return "SightMap";
  if (u.includes("realpage.com") || u.includes("onesite.com")) return "RealPage/OneSite";
  if (u.includes("entrata.com")) return "Entrata/ProspectPortal";
  if (u.includes("yardi.com") || u.includes("rentcafe")) return "Yardi/RentCafe";
  if (u.includes("appfolio.com")) return "AppFolio";
  if (u.includes("resman") || u.includes("resman.app")) return "ResMan";
  if (u.includes("tenantcloud")) return "TenantCloud";
  return "";
}

function guessPlatformFromHtml(html) {
  const h = (html || "").toLowerCase();

  // RentCafe/Yardi patterns
  if (h.includes("securecafe") || h.includes("rentcafe") || h.includes("myolepropertyid")) return "RentCafe/SecureCafe";

  // SightMap patterns
  if (h.includes("sightmap") || h.includes("/app/api/v1/") || h.includes("sightmap.com/share/")) return "SightMap";

  // RealPage patterns
  if (h.includes("realpage") || h.includes("onesite") || h.includes("lead2lease") || h.includes("prospectportal")) return "RealPage/OneSite";

  // Entrata / ProspectPortal (often Cloudflare-protected)
  if (h.includes("entrata") || h.includes("prospectportal")) return "Entrata/ProspectPortal";

  // AppFolio
  if (h.includes("appfolio")) return "AppFolio";

  return "";
}

async function sniffUrl(url) {
  if (!url) return { http_status: "", platform_guess: "", notes: "No URL" };

  let http_status = "";
  let notes = "";
  let platform_guess = guessPlatformFromUrl(url);

  try {
    const res = await fetchWithTimeout(url, { method: "GET", redirect: "follow" }, 20000);
    http_status = String(res.status);

    // Only read body for HTML-ish pages
    const ctype = (res.headers.get("content-type") || "").toLowerCase();
    if (ctype.includes("text/html")) {
      const html = await res.text();
      const fromHtml = guessPlatformFromHtml(html);
      if (fromHtml) platform_guess = fromHtml || platform_guess;

      // note if cloudflare block suspected
      if (html.toLowerCase().includes("cloudflare") && html.toLowerCase().includes("attention required")) {
        notes = "Possible Cloudflare protection";
      }
    } else {
      // keep URL-based guess only
      if (!platform_guess) notes = `Non-HTML content-type: ${ctype || "unknown"}`;
    }
  } catch (e) {
    notes = `Fetch failed: ${e.message || String(e)}`;
  }

  return { http_status, platform_guess: platform_guess || "", notes };
}

// ---------- Main ----------

const inputCsv = readText(inputPath);
const { headers: inHeaders, rows: inRows } = parseCSV(inputCsv);

const required = ["zip", "assoc_code", "address"];
for (const r of required) {
  if (!inHeaders.includes(r)) {
    console.error(`Input CSV missing required header: ${r}`);
    console.error(`Found headers: ${inHeaders.join(", ")}`);
    process.exit(1);
  }
}

const outHeaders = [
  "zip",
  "assoc_code",
  "address",
  "url",
  "platform_guess",
  "http_status",
  "notes",
];

const outRows = [];

for (const row of inRows) {
  const zip = row.zip || "";
  const assoc = row.assoc_code || "";
  const address = row.address || "";

  const out = {
    zip,
    assoc_code: assoc,
    address,
    url: row.url || "",
    platform_guess: "",
    http_status: "",
    notes: "",
  };

  // 1) If no URL provided, try Places fallback lookup
  if (!out.url) {
    try {
      const found = await findWebsiteForAddress(apiKey, address);
      if (found?.website) {
        out.url = found.website;
        out.notes = `Website via Places (${found.name}) | query="${found.queryUsed}"`.trim();
      } else {
        out.notes = "No websiteUri found via Places fallback queries | No website URL";
      }
    } catch (err) {
      out.notes = `${err.message || String(err)} | No website URL`;
    }
  }

  // 2) If we have a URL, sniff platform
  if (out.url) {
    const sniff = await sniffUrl(out.url);
    out.platform_guess = sniff.platform_guess || "";
    out.http_status = sniff.http_status || "";
    // append notes (keep earlier lookup note)
    if (sniff.notes) {
      out.notes = out.notes ? `${out.notes} | ${sniff.notes}` : sniff.notes;
    }
    if (!out.platform_guess) {
      out.platform_guess = guessPlatformFromUrl(out.url) || "";
    }
  }

  outRows.push(out);
}

writeText(outputPath, toCSV(outHeaders, outRows));

console.log(`Done. Wrote: ${outputPath}`);
