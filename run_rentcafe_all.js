// run_rentcafe_all.js (HARDENED v7)
// Pull all RentCafe properties from Supabase and write daily unit snapshots (available-units only)

import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { chromium } from "playwright";

// ===== ENV =====
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;

const SNAPSHOT_DATE = process.env.SNAPSHOT_DATE || todayYYYYMMDD();

// Controls
const MAX_PROPERTIES = Number(process.env.RENTCAFE_MAX_PROPERTIES || 9999);
const HEADLESS = String(process.env.RENTCAFE_HEADLESS || "true").toLowerCase() !== "false";
const SLOW_MO_MS = Number(process.env.RENTCAFE_SLOWMO_MS || 0);
const PER_PAGE_DELAY_MS = Number(process.env.RENTCAFE_PER_PAGE_DELAY_MS || 250);

// Navigation hardening
const NAV_TIMEOUT_MS = Number(process.env.RENTCAFE_NAV_TIMEOUT_MS || 90000); // default 90s
const URL_RETRIES = Number(process.env.RENTCAFE_URL_RETRIES || 2); // retries per URL
const PROPERTY_MAX_SECONDS = Number(process.env.RENTCAFE_PROPERTY_MAX_SECONDS || 240); // kill-switch per property

// Debug capture
const DEBUG_DIR = process.env.RENTCAFE_DEBUG_DIR || "rentcafe_debug";
const SAVE_DEBUG_ON_FAIL = String(process.env.RENTCAFE_SAVE_DEBUG_ON_FAIL || "true").toLowerCase() !== "false";

// IMPORTANT: floorplan fallback intentionally OFF (we want true available units only)
const ENABLE_FLOORPLAN_FALLBACK =
  String(process.env.RENTCAFE_ENABLE_FLOORPLAN_FALLBACK || "").toLowerCase() === "true";

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in .env");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: { persistSession: false },
});

// ===== Helpers =====
function todayYYYYMMDD() {
  const d = new Date();
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
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

function firstMatch(text, regex) {
  const m = text.match(regex);
  return m ? m[1] : null;
}

function bedsHintFromUrl(url) {
  const u = String(url || "").toLowerCase();
  if (u.includes("studio")) return 0;
  if (u.includes("single")) return 0;
  if (u.includes("one-bedroom") || u.includes("1-bedroom") || u.includes("1bed")) return 1;
  if (u.includes("two-bedroom") || u.includes("2-bedroom") || u.includes("2bed")) return 2;
  if (u.includes("three-bedroom") || u.includes("3-bedroom") || u.includes("3bed")) return 3;
  return null;
}

function parseDateAvailableToISO(raw) {
  const s = String(raw || "").trim();
  const m = s.match(/\b(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})\b/);
  if (!m) return null;
  const mm = String(m[1]).padStart(2, "0");
  const dd = String(m[2]).padStart(2, "0");
  const yyyy = m[3];
  return `${yyyy}-${mm}-${dd}`;
}

function normalizeCommaUrls(s) {
  return String(s || "")
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);
}

function safeFilePart(s) {
  return String(s || "")
    .replace(/^https?:\/\//, "")
    .replace(/[^\w.-]+/g, "_")
    .slice(0, 160);
}

async function ensureDir(path) {
  const fs = await import("node:fs/promises");
  await fs.mkdir(path, { recursive: true }).catch(() => {});
}

// ===== DOM Units Extraction (STRICT) =====
async function extractAvailableUnitsFromDom(page, pageUrl) {
  await Promise.race([
    page.waitForSelector("#availApts", { timeout: 4500 }).catch(() => null),
    page.waitForSelector('[id*="avail"]', { timeout: 4500 }).catch(() => null),
    page.waitForSelector("text=Available Units", { timeout: 4500 }).catch(() => null),
  ]);

  const bedsHint = bedsHintFromUrl(pageUrl);

  const rawBlocks = await page.evaluate(() => {
    const root =
      document.querySelector("#availApts") ||
      document.querySelector('[id*="avail"]') ||
      document.body;

    const candidates = Array.from(
      root.querySelectorAll(
        "li, .row, .card, .available, [class*='avail'], [class*='unit'], [class*='apartment']"
      )
    );

    const blocks = candidates
      .map((el) => (el.innerText || "").replace(/\s+/g, " ").trim())
      .filter((t) => t.length >= 20);

    const seen = new Set();
    const out = [];
    for (const b of blocks) {
      const key = b.slice(0, 180);
      if (seen.has(key)) continue;
      seen.add(key);
      out.push(b);
      if (out.length >= 250) break;
    }
    return out;
  });

  const results = [];
  const seen = new Set();

  for (const block of rawBlocks) {
    const lower = block.toLowerCase();

    // Skip non-unit availability marketing
    if (
      lower.includes("add to waitlist") ||
      lower.includes("waitlist") ||
      lower.includes("inquire for details") ||
      lower.includes("call for details") ||
      lower.includes("contact us")
    ) {
      continue;
    }

    const unitNum =
      firstMatch(block, /Apartment\s*:\s*#\s*([A-Za-z0-9-]+)/i) ||
      firstMatch(block, /Apartment\s*#\s*([A-Za-z0-9-]+)/i) ||
      firstMatch(block, /\bUnit\s*#?\s*([A-Za-z0-9-]+)/i) ||
      firstMatch(block, /\b#\s*([A-Za-z0-9-]{2,20})\b/);

    // Require unit number
    if (!unitNum) continue;

    const priceText =
      firstMatch(block, /Starting\s+at\s*:\s*(\$[0-9,]+)/i) || firstMatch(block, /(\$[0-9,]+)/);

    const dateAvailRaw = firstMatch(
      block,
      /Date\s*Available\s*:\s*([0-9]{1,2}[\/\-][0-9]{1,2}[\/\-][0-9]{4})/i
    );
    const dateAvailIso = dateAvailRaw ? parseDateAvailableToISO(dateAvailRaw) : null;

    const availableText = dateAvailIso || firstMatch(block, /(Available\s+Now)/i) || null;

    // Require at least price or availability
    if (!priceText && !availableText) continue;

    const key = `${unitNum}|${priceText || ""}|${availableText || ""}`;
    if (seen.has(key)) continue;
    seen.add(key);

    results.push({
      unit_id: `unit:${unitNum}`,
      unit_number: unitNum,
      available_on: availableText || null,
      price: priceText ? parseMoneyToNumber(priceText) : null,
      floor_plan_id: null,
      meta: {
        source: "RentCafe",
        page_url: pageUrl,
        beds_hint: bedsHint,
        price_text: priceText || null,
        raw: block,
      },
    });
  }

  return results;
}

// ===== OPTIONAL Floorplan fallback (not recommended) =====
function stripTags(html) {
  return html
    .replace(/<script[\s\S]*?<\/script>/gi, "")
    .replace(/<style[\s\S]*?<\/style>/gi, "")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}
function parseFloorplansFromHtml(html) {
  const markers = [
    "fp-card",
    "floorplan-card",
    "floorplan-item",
    "fpWrapper",
    "js-floorplan",
    "floorplanName",
    "Floor Plan",
  ];

  let blocks = [];
  for (const mk of markers) {
    const parts = html.split(mk);
    if (parts.length >= 3) {
      blocks = parts.slice(1).map((p) => mk + p).slice(0, 250);
      break;
    }
  }

  const results = [];
  const seen = new Set();

  for (const b of blocks) {
    const text = stripTags(b);

    const name =
      firstMatch(text, /Floor\s*Plan\s*[:\-]\s*([A-Za-z0-9][A-Za-z0-9 \-_]+)/i) ||
      firstMatch(text, /^([A-Za-z0-9][A-Za-z0-9 \-_]{1,40})\s+(Studio|[0-9]\s*Bed)/i);

    if (!name) continue;

    const key = name;
    if (seen.has(key)) continue;
    seen.add(key);

    results.push({
      unit_id: `floorplan:${name}`,
      unit_number: name,
      available_on: null,
      price: null,
      floor_plan_id: null,
      meta: { source: "RentCafe", fallback: true },
    });
  }

  return results;
}

// ===== Playwright shared browser =====
async function createBrowser() {
  return chromium.launch({ headless: HEADLESS, slowMo: SLOW_MO_MS });
}

async function gotoWithRetries(page, url, { maxRetries = 2, timeoutMs = 60000 } = {}) {
  let lastErr = null;

  // Two strategies: domcontentloaded first, then networkidle
  const strategies = [
    { waitUntil: "domcontentloaded" },
    { waitUntil: "networkidle" },
  ];

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    for (const strat of strategies) {
      try {
        const resp = await page.goto(url, { timeout: timeoutMs, ...strat });
        return resp;
      } catch (err) {
        lastErr = err;
        // continue
      }
    }
    // backoff between attempts
    if (attempt < maxRetries) await sleep(1200 + attempt * 800);
  }

  throw lastErr || new Error("Navigation failed");
}

async function captureDebug(page, tag) {
  if (!SAVE_DEBUG_ON_FAIL) return;
  await ensureDir(DEBUG_DIR);

  const fs = await import("node:fs/promises");
  const base = `${DEBUG_DIR}/${tag}`;

  try {
    await page.screenshot({ path: `${base}.png`, fullPage: true }).catch(() => {});
  } catch {}

  try {
    const html = await page.content().catch(() => "");
    if (html) await fs.writeFile(`${base}.html`, html, "utf8");
  } catch {}
}

async function scrapeUrlsWithSharedBrowser(browser, urls, debugTagPrefix) {
  const context = await browser.newContext({
    userAgent:
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    viewport: { width: 1280, height: 800 },
    locale: "en-US",
  });

  const page = await context.newPage();

  // Reduce “automation” signals slightly
  await page.addInitScript(() => {
    Object.defineProperty(navigator, "webdriver", { get: () => false });
  });

  const all = [];
  const seen = new Set();
  const byPage = {};

  try {
    for (const url of urls) {
      const tag = `${debugTagPrefix}__${safeFilePart(url)}`;

      console.log("  Opening:", url);
      let resp = null;

      try {
        resp = await gotoWithRetries(page, url, { maxRetries: URL_RETRIES, timeoutMs: NAV_TIMEOUT_MS });
      } catch (err) {
        console.log("  ❌ goto failed:", err?.message || err);
        byPage[url] = 0;
        await captureDebug(page, `${tag}__goto_fail`);
        continue; // move on to next URL
      }

      console.log("  HTTP status:", resp ? resp.status() : null);

      // Small settle time; many sites keep loading ads/trackers forever
      await page.waitForTimeout(2000);

      let domUnits = [];
      try {
        domUnits = await extractAvailableUnitsFromDom(page, url);
      } catch (err) {
        console.log("  ⚠️ extract failed:", err?.message || err);
        await captureDebug(page, `${tag}__extract_fail`);
      }

      if (domUnits.length) {
        byPage[url] = domUnits.length;
        for (const u of domUnits) {
          if (!u?.unit_id) continue;
          if (seen.has(u.unit_id)) continue;
          seen.add(u.unit_id);
          all.push(u);
        }
      } else if (ENABLE_FLOORPLAN_FALLBACK) {
        try {
          const html = await page.content();
          const fp = parseFloorplansFromHtml(html);
          byPage[url] = fp.length;
          for (const u of fp) {
            if (!u?.unit_id) continue;
            if (seen.has(u.unit_id)) continue;
            seen.add(u.unit_id);
            all.push(u);
          }
        } catch (err) {
          byPage[url] = 0;
          console.log("  ⚠️ fallback parse failed:", err?.message || err);
          await captureDebug(page, `${tag}__fallback_fail`);
        }
      } else {
        byPage[url] = 0;
      }

      await sleep(PER_PAGE_DELAY_MS);
    }
  } finally {
    await page.close().catch(() => {});
    await context.close().catch(() => {});
  }

  return { units: all, byPage };
}

// ===== Supabase IO =====

// Tries to select optional skip columns if they exist
async function fetchRentCafeProperties() {
  // You store URLs in properties.rentcafe_urls (text with comma-separated URLs)
  // Optional columns supported if present:
  // - skip_scrape (bool)
  // - skip_reason (text)
  const baseCols = ["id", "name", "platform", "rentcafe_urls"];
  const optionalCols = ["skip_scrape", "skip_reason"];

  let selectCols = baseCols.join(",");
  // attempt to include optionals; if Supabase errors, retry without them
  const trySelect = async (cols) =>
    supabase.from("properties").select(cols).ilike("platform", "rentcafe").limit(MAX_PROPERTIES);

  let res = await trySelect([...baseCols, ...optionalCols].join(","));
  if (res.error) {
    res = await trySelect(selectCols);
  }

  if (res.error) throw res.error;
  return res.data || [];
}

async function upsertSnapshot({ propertyId, snapshotDate, units }) {
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

// ===== Main =====
async function main() {
  console.log("RentCafe runner starting (v7 hardened)...");
  console.log("Snapshot date:", SNAPSHOT_DATE);
  console.log("Headless:", HEADLESS);
  console.log("Floorplan fallback enabled:", ENABLE_FLOORPLAN_FALLBACK);
  console.log("NAV_TIMEOUT_MS:", NAV_TIMEOUT_MS, "URL_RETRIES:", URL_RETRIES);

  const props = await fetchRentCafeProperties();
  console.log(`Found RentCafe properties: ${props.length}`);

  if (!props.length) {
    console.log("No RentCafe properties found. Ensure properties.platform is 'rentcafe' or matches ilike('rentcafe').");
    process.exit(0);
  }

  await ensureDir(DEBUG_DIR);

  const browser = await createBrowser();

  let ok = 0;
  let fail = 0;
  let skipped = 0;

  try {
    for (const p of props) {
      const propertyId = p.id;
      const name = p.name || "(unnamed)";
      const urls = normalizeCommaUrls(p.rentcafe_urls);

      // Optional skip columns
      if (p.skip_scrape === true) {
        console.log(`\n[SKIP] ${name} (${propertyId}) — skip_scrape=true ${p.skip_reason ? `(${p.skip_reason})` : ""}`);
        skipped += 1;
        continue;
      }

      if (!urls.length) {
        console.log(`\n[SKIP] ${name} (${propertyId}) — rentcafe_urls is empty`);
        skipped += 1;
        continue;
      }

      console.log(`\n[RUN] ${name} (${propertyId})`);
      console.log("  URLs:", urls);

      const debugPrefix = `${safeFilePart(name)}__${propertyId}__${SNAPSHOT_DATE}`;

      // Kill-switch per property (prevents one property from eating the whole run)
      const startedAt = Date.now();
      const maxMs = PROPERTY_MAX_SECONDS * 1000;

      try {
        const { units, byPage } = await Promise.race([
          scrapeUrlsWithSharedBrowser(browser, urls, debugPrefix),
          (async () => {
            await sleep(maxMs);
            throw new Error(`Property timeout exceeded (${PROPERTY_MAX_SECONDS}s)`);
          })(),
        ]);

        console.log("  Units by page:", byPage);
        console.log("  Units extracted:", units.length);

        if (!units.length) {
          console.log("  ⚠️ No available units found (may be correct). Still writing snapshot.");
        }

        const row = await upsertSnapshot({
          propertyId,
          snapshotDate: SNAPSHOT_DATE,
          units,
        });

        console.log("  ✅ Snapshot upserted:", row.id);
        ok += 1;
      } catch (err) {
        const elapsed = Math.round((Date.now() - startedAt) / 1000);
        console.log(`  ❌ Failed after ${elapsed}s:`, err?.message || err);

        // Create a small marker file for quick triage
        const fs = await import("node:fs/promises");
        const marker = `${DEBUG_DIR}/${debugPrefix}__PROPERTY_FAIL.txt`;
        await fs
          .writeFile(marker, `FAILED: ${name}\nproperty_id=${propertyId}\nerr=${err?.message || err}\n`, "utf8")
          .catch(() => {});

        fail += 1;
      }

      // small delay between properties
      await sleep(400);
    }
  } finally {
    await browser.close().catch(() => {});
  }

  console.log("\nDone.");
  console.log("Success:", ok);
  console.log("Failed:", fail);
  console.log("Skipped:", skipped);
}

main()
  .then(() => setTimeout(() => process.exit(0), 200))
  .catch((err) => {
    console.error("❌ Runner failed:", err?.message || err);
    setTimeout(() => process.exit(1), 200);
  });
