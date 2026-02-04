// tools/detect_platform_batch.js
// Usage: node tools/detect_platform_batch.js input.csv output.csv

const fs = require("fs");
const https = require("https");
const http = require("http");
const { URL } = require("url");

function die(msg) {
  console.error(msg);
  process.exit(1);
}

function parseCSV(text) {
  // simple CSV parser that handles quoted fields
  const rows = [];
  let row = [];
  let cur = "";
  let inQuotes = false;

  for (let i = 0; i < text.length; i++) {
    const ch = text[i];
    const next = text[i + 1];

    if (ch === '"' && inQuotes && next === '"') {
      cur += '"';
      i++;
      continue;
    }
    if (ch === '"') {
      inQuotes = !inQuotes;
      continue;
    }
    if (ch === "," && !inQuotes) {
      row.push(cur.trim());
      cur = "";
      continue;
    }
    if ((ch === "\n" || ch === "\r") && !inQuotes) {
      if (ch === "\r" && next === "\n") i++;
      row.push(cur.trim());
      cur = "";
      if (row.length > 1 || row[0] !== "") rows.push(row);
      row = [];
      continue;
    }
    cur += ch;
  }
  if (cur.length || row.length) {
    row.push(cur.trim());
    rows.push(row);
  }

  const header = rows.shift();
  if (!header) return [];
  const headers = header.map((h) => h.replace(/^\uFEFF/, "").trim());

  return rows.map((r) => {
    const obj = {};
    headers.forEach((h, idx) => (obj[h] = (r[idx] ?? "").trim()));
    return obj;
  });
}

function toCSV(rows, headers) {
  const escape = (v) => {
    const s = String(v ?? "");
    if (/[,"\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
    return s;
  };
  const lines = [];
  lines.push(headers.map(escape).join(","));
  for (const row of rows) lines.push(headers.map((h) => escape(row[h])).join(","));
  return lines.join("\n") + "\n";
}

function detectPlatformFromUrl(urlStr) {
  const u = urlStr.toLowerCase();

  // RentCafe / SecureCafe (Yardi)
  if (u.includes("securecafe.com") || u.includes("rentcafe.com")) return "RentCafe/SecureCafe";

  // SightMap
  if (u.includes("sightmap.com")) return "SightMap";

  // RealPage / OneSite / ILM / ProspectPortal (often Cloudflare)
  if (u.includes("prospectportal.com") || u.includes("realpage.com") || u.includes("onesite")) return "RealPage/ProspectPortal";

  // Entrata
  if (u.includes("entrata.com") || u.includes("lead2lease.com") || u.includes("propertyboss") || u.includes("entrata")) return "Entrata";

  // Yardi Voyager / Yardi RentCafe already covered; but some use yardi.* domains
  if (u.includes("yardi") || u.includes("voyager")) return "Yardi (Other)";

  // ResMan
  if (u.includes("resman") || u.includes("resmanapp")) return "ResMan";

  // AppFolio
  if (u.includes("appfolio")) return "AppFolio";

  return "";
}

function fetchUrl(urlStr, timeoutMs = 12000) {
  return new Promise((resolve) => {
    try {
      const u = new URL(urlStr);
      const lib = u.protocol === "https:" ? https : http;

      const req = lib.get(
        urlStr,
        {
          headers: {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) PlatformDetector/1.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
          },
        },
        (res) => {
          let data = "";
          res.on("data", (chunk) => {
            data += chunk;
            // don’t download megabytes
            if (data.length > 1_200_000) res.destroy();
          });
          res.on("end", () => resolve({ status: res.statusCode || 0, body: data, finalUrl: urlStr }));
        }
      );

      req.on("error", () => resolve({ status: 0, body: "", finalUrl: urlStr }));
      req.setTimeout(timeoutMs, () => {
        req.destroy();
        resolve({ status: 0, body: "", finalUrl: urlStr });
      });
    } catch {
      resolve({ status: 0, body: "", finalUrl: urlStr });
    }
  });
}

function detectPlatformFromHtml(html) {
  const h = html.toLowerCase();

  // RentCafe/SecureCafe signatures
  if (h.includes("securecafe") || h.includes("rentcafe") || h.includes("myolepropertyid")) return "RentCafe/SecureCafe";

  // SightMap signatures
  if (h.includes("sightmap") || h.includes("/app/api/v1/") || h.includes("sightmaps/") || h.includes("share_landing_page")) return "SightMap";

  // Entrata signatures
  if (h.includes("entrata") || h.includes("lead2lease")) return "Entrata";

  // RealPage/ProspectPortal signatures
  if (h.includes("prospectportal") || h.includes("realpage") || h.includes("onesite")) return "RealPage/ProspectPortal";

  return "";
}

async function main() {
  const [, , inputPath, outputPath] = process.argv;
  if (!inputPath || !outputPath) die("Usage: node tools/detect_platform_batch.js input.csv output.csv");

  if (!fs.existsSync(inputPath)) die(`Input not found: ${inputPath}`);

  const text = fs.readFileSync(inputPath, "utf8");
  const rows = parseCSV(text);

  if (!rows.length) die("No rows found in input.");

  const out = [];

  for (const r of rows) {
    const zip = r.zip || "";
    const assoc_code = r.assoc_code || r.code || r.internal || "";
    const address = r.address || "";
    const url = r.url || "";

    let platform_guess = "";
    let http_status = "";
    let notes = "";

    if (url) {
      platform_guess = detectPlatformFromUrl(url) || platform_guess;

      const { status, body } = await fetchUrl(url);
      http_status = String(status);

      // If URL-pattern guess was empty, try HTML-based guess
      const htmlGuess = detectPlatformFromHtml(body);
      if (!platform_guess && htmlGuess) platform_guess = htmlGuess;

      if (!platform_guess) notes = "Could not detect from URL/HTML";
    } else {
      notes = "No URL provided (lookup step not implemented yet)";
    }

    out.push({
      zip,
      assoc_code,
      address,
      url,
      platform_guess,
      http_status,
      notes,
    });
  }

  const headers = ["zip", "assoc_code", "address", "url", "platform_guess", "http_status", "notes"];
  fs.writeFileSync(outputPath, toCSV(out, headers), "utf8");
  console.log(`✅ Wrote ${out.length} rows to ${outputPath}`);
}

main().catch((e) => die(`Fatal: ${e?.message || e}`));
