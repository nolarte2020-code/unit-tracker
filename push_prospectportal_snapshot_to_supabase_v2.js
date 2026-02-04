// push_prospectportal_snapshot_to_supabase_v2.js
// ProspectPortal (Entrata) runner: scrape -> snapshot upsert -> diff -> unit_events insert
// v2 adds: better JSON sniffer filters + HTML save + endpoint discovery logging

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { chromium } from "playwright";
import fs from "fs";

// ================== ENV ==================
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const PROPERTY_ID = process.env.PROPERTY_ID; // Supabase properties.id UUID
const PROSPECTPORTAL_URL = process.env.PROSPECTPORTAL_URL;

const EVENT_SOURCE = process.env.EVENT_SOURCE || "prospectportal";
const SNAPSHOT_DATE = process.env.SNAPSHOT_DATE; // YYYY-MM-DD optional

const PROSPECTPORTAL_SNIFF_JSON =
  String(process.env.PROSPECTPORTAL_SNIFF_JSON || "").toLowerCase() === "true";

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in env");
  process.exit(1);
}
if (!PROPERTY_ID) {
  console.error("Missing PROPERTY_ID in env");
  process.exit(1);
}
if (!PROSPECTPORTAL_URL) {
  console.error("Missing PROSPECTPORTAL_URL in env");
  process.exit(1);
}

// ================== SUPABASE ==================
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

// ================== HELPERS ==================
function todayYYYYMMDD() {
  const d = new Date();
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

function toNumberMaybe(s) {
  if (s == null) return null;
  const cleaned = String(s).replace(/[^\d.]/g, "");
  if (!cleaned) return null;
  const n = Number(cleaned);
  return Number.isFinite(n) ? n : null;
}

function parseMoneyToNumber(s) {
  return toNumberMaybe(s);
}

function normalizeUnitNumber(x) {
  const n = String(x ?? "").trim();
  return n ? n : null;
}

function normalizeUnitKeyFromNumber(unitNumber) {
  const n = normalizeUnitNumber(unitNumber);
  if (!n) return null;
  return n.startsWith("unit:") ? n : `unit:${n}`;
}

function unitNumberFromUnitKey(unitKey) {
  if (!unitKey) return null;
  const s = String(unitKey);
  if (s.startsWith("unit:")) return s.slice(5) || null;
  return s || null;
}

function diffKeys(prevUnits, currUnits) {
  const prevSet = new Set((prevUnits || []).map((u) => u.unit_key).filter(Boolean));
  const currSet = new Set((currUnits || []).map((u) => u.unit_key).filter(Boolean));
  const appeared = [...currSet].filter((k) => !prevSet.has(k));
  const disappeared = [...prevSet].filter((k) => !currSet.has(k));
  return { appeared, disappeared };
}

function safeWriteFile(name, content) {
  try {
    fs.writeFileSync(name, content, "utf8");
    return true;
  } catch {
    return false;
  }
}

function uniq(arr) {
  return [...new Set(arr)];
}

function looksInventoryishUrl(url) {
  const u = String(url || "").toLowerCase();
  return (
    u.includes("prospectportal.com") &&
    (u.includes("availability") ||
      u.includes("available") ||
      u.includes("floorplan") ||
      u.includes("floorplans") ||
      u.includes("unit") ||
      u.includes("units") ||
      u.includes("conventional") ||
      u.includes("api") ||
      u.includes("search"))
  );
}

function isNoiseHost(host) {
  const h = String(host || "").toLowerCase();
  return (
    h.includes("beans.ai") || // IMPORTANT: ignore Beans.ai
    h.includes("cookielaw") ||
    h.includes("googletagmanager") ||
    h.includes("google-analytics") ||
    h.includes("doubleclick") ||
    h.includes("hotjar") ||
    h.includes("facebook") ||
    h.includes("sentry") ||
    h.includes("datadog")
  );
}

function discoverCandidateUrlsFromHtml(html, baseUrl) {
  const found = [];
  const re = /(https?:\/\/[^\s"'<>]+)|(["']\/[^\s"'<>]+["'])/g;
  let m;
  while ((m = re.exec(html)) !== null) {
    const abs = m[1] ? m[1] : null;
    const rel = m[2] ? m[2].slice(1, -1) : null;
    try {
      const u = abs ? abs : new URL(rel, baseUrl).toString();
      if (looksInventoryishUrl(u)) found.push(u);
    } catch {}
  }
  return uniq(found).slice(0, 80);
}

// ================== JSON SNIFFER (FILTERED) ==================
function attachJsonSniffer(page) {
  let saved = 0;

  page.on("response", async (res) => {
    try {
      const ct = (res.headers()["content-type"] || "").toLowerCase();
      if (!ct.includes("application/json") && !ct.includes("text/json")) return;

      const url = res.url();
      const host = new URL(url).host;

      if (isNoiseHost(host)) return;
      if (!looksInventoryishUrl(url)) return;

      const body = await res.text();
      if (!body || body.length < 200) return;

      saved += 1;
      if (saved <= 25) {
        const safeHost = host.replaceAll(".", "_");
        const file = `prospectportal_json_${String(saved).padStart(2, "0")}_${safeHost}.txt`;
        safeWriteFile(file, `URL: ${url}\n\n${body}`);
        console.log("Saved JSON:", file);
      }
    } catch {}
  });
}

// ================== SCRAPE ==================
async function scrapeProspectPortalUnits(listingUrl) {
  const browser = await chromium.launch({ headless: true });

  const context = await browser.newContext({
    userAgent:
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    viewport: { width: 1280, height: 800 },
    locale: "en-US",
  });

  const page = await context.newPage();

  if (PROSPECTPORTAL_SNIFF_JSON) attachJsonSniffer(page);

  try {
    console.log("Opening listing:", listingUrl);
    const resp = await page.goto(listingUrl, { waitUntil: "domcontentloaded", timeout: 60000 });
    console.log("HTTP status:", resp ? resp.status() : null);

    await page.waitForTimeout(2500);

    // Save listing HTML for debugging
    const html = await page.content();
    safeWriteFile("prospectportal_listing.html", html);
    console.log("Saved listing HTML: prospectportal_listing.html");

    // Discover candidate endpoints from HTML
    const discovered = discoverCandidateUrlsFromHtml(html, listingUrl);
    if (discovered.length) {
      safeWriteFile("prospectportal_discovered_urls.txt", discovered.join("\n"));
      console.log("Discovered candidate URLs:", discovered.length);
      console.log("Saved: prospectportal_discovered_urls.txt");
    } else {
      console.log("No candidate URLs discovered from HTML.");
    }

    // Collect likely internal links from DOM too
    const detailLinks = await page.evaluate(() => {
      const anchors = Array.from(document.querySelectorAll("a[href]"));
      const links = anchors
        .map((a) => a.getAttribute("href") || "")
        .filter(Boolean)
        .map((h) => h.trim());

      const isLikely = (h) => {
        const u = h.toLowerCase();
        return (
          u.includes("conventional") ||
          u.includes("floorplan") ||
          u.includes("availability") ||
          u.includes("apartments") ||
          u.includes("details") ||
          u.includes("unit")
        );
      };

      return links.filter(isLikely).slice(0, 250);
    });

    const abs = new Set();
    const base = new URL(listingUrl);
    for (const h of detailLinks) {
      try {
        const u = new URL(h, base).toString();
        if (new URL(u).host === base.host) abs.add(u);
      } catch {}
    }

    const urls = [...abs];
    console.log("Candidate detail URLs:", urls.length);

    // v2 still tries light text extraction (may be 0; that’s OK)
    const units = [];
    const seen = new Set();

    for (const u of urls) {
      try {
        await page.goto(u, { waitUntil: "domcontentloaded", timeout: 60000 });
        await page.waitForTimeout(1200);

        const extracted = await page.evaluate((pageUrl) => {
          const raw = (document.body?.innerText || "").replace(/\u00a0/g, " ").replace(/\r/g, "");
          const lines = raw
            .split("\n")
            .map((l) => l.replace(/\s+/g, " ").trim())
            .filter(Boolean);

          const unitRegexes = [
            /\bUnit\s*#?\s*([A-Za-z0-9-]{1,15})\b/i,
            /\bApt\s*#?\s*([A-Za-z0-9-]{1,15})\b/i,
            /\bApartment\s*#?\s*([A-Za-z0-9-]{1,15})\b/i,
            /\b#\s*([A-Za-z0-9-]{1,15})\b/,
          ];

          const priceRegex = /\$[0-9,]{3,7}(?:\s*-\s*\$[0-9,]{3,7})?/;
          const dateRegex = /\b(\d{1,2}\/\d{1,2}\/\d{4})\b/;
          const availWord = /\bAvailable\b/i;

          const out = [];

          // rolling windows to catch split layouts
          const windows = [];
          for (let i = 0; i < lines.length; i++) {
            windows.push(lines[i]);
            if (i + 1 < lines.length) windows.push(lines[i] + " " + lines[i + 1]);
            if (i + 2 < lines.length)
              windows.push(lines[i] + " " + lines[i + 1] + " " + lines[i + 2]);
          }

          for (const w of windows) {
            let unit = null;
            for (const r of unitRegexes) {
              const m = w.match(r);
              if (m && m[1]) {
                unit = m[1];
                break;
              }
            }
            if (!unit) continue;

            const pm = w.match(priceRegex);
            const price_text = pm ? pm[0] : null;

            const dm = w.match(dateRegex);
            const available_on = dm ? dm[1] : availWord.test(w) ? "Available" : null;

            if (!price_text && !available_on) continue;

            out.push({ unit_number: unit, price_text, available_on, raw: w, page_url: pageUrl });
          }

          // dedupe within page
          const seen = new Set();
          const uniq = [];
          for (const r of out) {
            const k = (r.unit_number || "").toLowerCase();
            if (!k || seen.has(k)) continue;
            seen.add(k);
            uniq.push(r);
          }
          return uniq.slice(0, 200);
        }, u);

        for (const r of extracted) {
          const unit_number = normalizeUnitNumber(r.unit_number);
          const unit_key = normalizeUnitKeyFromNumber(unit_number);
          if (!unit_key) continue;

          if (seen.has(unit_key)) continue;
          seen.add(unit_key);

          units.push({
            unit_key,
            unit_id: unit_key,
            unit_number,
            available_on: r.available_on || null,
            price: r.price_text ? parseMoneyToNumber(r.price_text) : null,
            floor_plan_id: null,
            meta: {
              source: "ProspectPortal",
              page_url: r.page_url,
              price_text: r.price_text || null,
              raw: r.raw || null,
            },
          });
        }
      } catch {
        // keep going
      }
    }

    return units;
  } finally {
    await page.close().catch(() => {});
    await context.close().catch(() => {});
    await browser.close().catch(() => {});
  }
}

// ================== SNAPSHOT + EVENTS WRITES ==================
async function getLatestSnapshot(propertyId, upToDateISO) {
  const { data, error } = await supabase
    .from("unit_snapshots")
    .select("snapshot_date, units_json")
    .eq("property_id", propertyId)
    .lte("snapshot_date", upToDateISO)
    .order("snapshot_date", { ascending: false })
    .limit(1);

  if (error) throw error;
  return data?.[0] || null;
}

async function upsertSnapshot({ propertyId, snapshotDate, units }) {
  const { data, error } = await supabase
    .from("unit_snapshots")
    .upsert(
      { property_id: propertyId, snapshot_date: snapshotDate, units_json: units },
      { onConflict: "property_id,snapshot_date" }
    )
    .select("id, property_id, snapshot_date, created_at")
    .single();

  if (error) throw error;
  return data;
}

async function replaceEventsForDay({ propertyId, eventDate, source, events }) {
  const { error: delErr } = await supabase
    .from("unit_events")
    .delete()
    .eq("property_id", propertyId)
    .eq("event_date", eventDate)
    .eq("source", source);

  if (delErr) throw delErr;

  if (!events.length) return;

  const { error: insErr } = await supabase.from("unit_events").insert(events);
  if (insErr) throw insErr;
}

// ================== MAIN ==================
async function main() {
  const snapshotDate = SNAPSHOT_DATE || todayYYYYMMDD();

  console.log("Fetching ProspectPortal data...");
  console.log("PROSPECTPORTAL_URL:", PROSPECTPORTAL_URL);

  const unitsToday = await scrapeProspectPortalUnits(PROSPECTPORTAL_URL);

  console.log("Units extracted:", unitsToday.length);

  if (!unitsToday.length) {
    console.log("Extracted 0 units. Use discovered URLs + filtered JSON sniffing to find inventory endpoint.");
    process.exit(2);
  }

  const prevSnap = await getLatestSnapshot(PROPERTY_ID, snapshotDate);
  const prevUnitsRaw = prevSnap?.units_json || [];

  const prevUnits = (prevUnitsRaw || [])
    .map((u) => {
      const unit_key =
        u?.unit_key ||
        normalizeUnitKeyFromNumber(u?.unit_number) ||
        (u?.unit_id?.startsWith("unit:") ? u.unit_id : null);

      const unit_number =
        u?.unit_number || unitNumberFromUnitKey(unit_key) || unitNumberFromUnitKey(u?.unit_id);

      return { ...u, unit_key, unit_number };
    })
    .filter((u) => u.unit_key);

  const { appeared, disappeared } = diffKeys(prevUnits, unitsToday);

  const unitKeyToNumber = new Map();
  for (const u of unitsToday) {
    if (u.unit_key && u.unit_number) unitKeyToNumber.set(u.unit_key, String(u.unit_number));
  }

  const events = [
    ...appeared.map((k) => ({
      property_id: PROPERTY_ID,
      event_date: snapshotDate,
      unit_key: k,
      unit_number: unitKeyToNumber.get(k) || unitNumberFromUnitKey(k),
      event_type: "appeared",
      source: EVENT_SOURCE,
    })),
    ...disappeared.map((k) => ({
      property_id: PROPERTY_ID,
      event_date: snapshotDate,
      unit_key: k,
      unit_number: unitKeyToNumber.get(k) || unitNumberFromUnitKey(k),
      event_type: "disappeared",
      source: EVENT_SOURCE,
    })),
  ];

  console.log(
    `Diff results for ${snapshotDate}: +${appeared.length} appeared, -${disappeared.length} disappeared`
  );

  console.log("Pushing snapshot to Supabase...");
  const row = await upsertSnapshot({
    propertyId: PROPERTY_ID,
    snapshotDate,
    units: unitsToday,
  });

  console.log(`Writing unit_events (source=${EVENT_SOURCE})...`);
  await replaceEventsForDay({
    propertyId: PROPERTY_ID,
    eventDate: snapshotDate,
    source: EVENT_SOURCE,
    events,
  });

  console.log("Snapshot upserted:", row);
  console.log("Events written:", events.length);
}

main()
  .then(() => (process.exitCode = 0))
  .catch((err) => {
    console.error("Failed:", err?.message || err);
    process.exitCode = 1;
  });
