import { useEffect, useMemo, useState } from "react";
import { supabase } from "./supabase";

/**
 * Canonical platform list used by BOTH Add + Edit.
 */
const PLATFORM_OPTIONS = [
  { value: "RentCafe", label: "RentCafe" },

  // Keep legacy / existing values
  { value: "yardi", label: "Yardi" },
  { value: "realpage", label: "RealPage" },
  { value: "sightmap", label: "SightMap" },
  { value: "entrata", label: "Entrata" },
  { value: "appfolio", label: "AppFolio" },

  { value: "rentmanager", label: "Rent Manager" },
  { value: "buildium", label: "Buildium" },
  { value: "resman", label: "ResMan" },
  { value: "knock", label: "Knock CRM" },
  { value: "leasehawk", label: "LeaseHawk" },

  { value: "wordpress", label: "WordPress" },
  { value: "webflow", label: "Webflow" },
  { value: "squarespace", label: "Squarespace" },
  { value: "html", label: "Custom HTML" },

  { value: "unknown", label: "Unknown" },
];

const LEAD_STATUS_OPTIONS = ["new","called","reached","follow_up","closed","lost"];

function normalizePlatformValue(v) {
  if (!v) return "unknown";
  const s = String(v).trim();
  const lower = s.toLowerCase();

  if (lower === "rentcafe") return "RentCafe";
  if (lower === "sightmap") return "sightmap";
  if (lower === "realpage") return "realpage";
  if (lower === "yardi") return "yardi";
  if (lower === "entrata") return "entrata";
  if (lower === "appfolio") return "appfolio";
  if (lower === "rentmanager") return "rentmanager";
  if (lower === "buildium") return "buildium";
  if (lower === "resman") return "resman";
  if (lower === "knock") return "knock";
  if (lower === "leasehawk") return "leasehawk";
  if (lower === "wordpress") return "wordpress";
  if (lower === "webflow") return "webflow";
  if (lower === "squarespace") return "squarespace";
  if (lower === "html") return "html";
  if (lower === "unknown") return "unknown";

  return s;
}

function platformLabel(value) {
  const v = normalizePlatformValue(value);
  const found = PLATFORM_OPTIONS.find((o) => o.value === v);
  return found ? found.label : String(value || "unknown");
}

function isoDateLocal(d = new Date()) {
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

function parseISODateLocal(iso) {
  // safe local parse (no timezone surprise)
  return new Date(`${iso}T00:00:00`);
}

function addDaysISO(iso, deltaDays) {
  const d = parseISODateLocal(iso);
  d.setDate(d.getDate() + deltaDays);
  return isoDateLocal(d);
}

function toISODateLocal(d) {
  const dt = new Date(d.getTime() - d.getTimezoneOffset() * 60000);
  return dt.toISOString().slice(0, 10);
}
function startOfMonth(d) { return new Date(d.getFullYear(), d.getMonth(), 1); }
function endOfMonth(d) { return new Date(d.getFullYear(), d.getMonth() + 1, 0); }
function getMonthGrid(d) {
  const start = startOfMonth(d);
  const end = endOfMonth(d);
  const gridStart = new Date(start);
  gridStart.setDate(start.getDate() - start.getDay());
  const gridEnd = new Date(end);
  gridEnd.setDate(end.getDate() + (6 - end.getDay()));
  const days = [];
  const cur = new Date(gridStart);
  while (cur <= gridEnd) { days.push(new Date(cur)); cur.setDate(cur.getDate() + 1); }
  return days;
}
function formatMonthTitle(d) {
  return d.toLocaleString(undefined, { month: "long", year: "numeric" });
}


function safeStr(v) {
  if (v === null || v === undefined) return "";
  return String(v);
}

function extractUnitNumber(u) {
  return (
    u?.unit_number ??
    u?.unitNumber ??
    u?.meta?.unit_number ??
    u?.meta?.unitNumber ??
    u?.unit ??
    u?.number ??
    null
  );
}

function extractUnitKey(u) {
  return u?.unit_key ?? u?.unitKey ?? u?.unit_id ?? u?.unitId ?? "";
}

function formatAddressLine(p) {
  const a = safeStr(p?.address).trim();
  const c = safeStr(p?.city).trim();
  const z = safeStr(p?.zip).trim();

  const parts = [];
  if (a) parts.push(a);
  const cityZip = [c, z].filter(Boolean).join(" ");
  if (cityZip) parts.push(cityZip);

  return parts.join(", ");
}

function uniqSorted(arr) {
  return Array.from(new Set(arr.filter((x) => safeStr(x).trim() !== ""))).sort((a, b) =>
    safeStr(a).localeCompare(safeStr(b))
  );
}

function chunkArray(arr, size) {
  const chunks = [];
  for (let i = 0; i < arr.length; i += size) chunks.push(arr.slice(i, i + size));
  return chunks;
}

/**
 * Fetch latest snapshot <= selectedDate for a set of property_ids
 * Returns Map(property_id -> snapshotRow)
 */
async function fetchLatestSnapshotsByProperty({ propertyIds, dateISO }) {
  const ids = propertyIds || [];
  const latest = new Map();
  if (ids.length === 0) return latest;

  const chunks = chunkArray(ids, 200);

  for (const chunk of chunks) {
    const { data, error } = await supabase
      .from("unit_snapshots")
      .select("property_id, snapshot_date, created_at, units_json")
      .in("property_id", chunk)
      .lte("snapshot_date", dateISO)
      .order("snapshot_date", { ascending: false })
      .order("created_at", { ascending: false });

    if (error) throw error;

    // Because ordered DESC, first time we see a property is its latest <= date.
    for (const row of data || []) {
      if (!latest.has(row.property_id)) latest.set(row.property_id, row);
    }
  }

  return latest;
}

/**
 * Build Move-Out Call Queue groups:
 * - NEW units on market = daily_units.event_type = 'appeared'
 * - Default date filter: TODAY ONLY
 * - Grouped by Rep -> Property -> Unit
 * - Deduplicate by property_id + unit_key (fallback unit_number)
 * - Use properties.rep_id as primary rep assignment (fallback daily_units.rep_id)
 *
 * NOTE: In rep-scoped mode, you can pass repScopedProperties and/or repId for filtering.
 */
function buildCallQueue({ dailyRows, properties, reps }) {
  const repNameById = new Map((reps || []).map((r) => [r.id, r.name]));
  const propById = new Map((properties || []).map((p) => [p.id, p]));

  const dedup = new Map(); // key -> normalized row
  for (const r of dailyRows || []) {
    const propertyId = r.property_id || "";
    const unitKey = safeStr(r.unit_key).trim();
    const unitNumber = safeStr(r.unit_number).trim();

    const dedupKey = `${propertyId}::${unitKey || unitNumber || "unknown"}`;
    if (dedup.has(dedupKey)) continue;

    const p = propById.get(propertyId) || null;

    const assignedRepId =
      (p && p.rep_id ? p.rep_id : "") || (r.rep_id ? r.rep_id : "") || "";

    const repName = assignedRepId ? (repNameById.get(assignedRepId) || "Unknown") : "Unassigned";

    dedup.set(dedupKey, {
      property_id: propertyId,
      property_name: (p && p.name) || r.property_name || "Unknown",
      address_line: p ? formatAddressLine(p) : "",
      rep_id: assignedRepId,
      rep_name: repName,
      unit_number: unitNumber || "",
      unit_key: unitKey || "",
      event_type: r.event_type || "",
      event_date: r.event_date || "",
      source: r.source || "",
    });
  }

  // Group: rep -> property -> unit
  const repGroups = new Map(); // rep_id -> {rep_id, rep_name, properties: Map}
  for (const row of dedup.values()) {
    const repId = row.rep_id || "";
    if (!repGroups.has(repId)) {
      repGroups.set(repId, {
        rep_id: repId,
        rep_name: row.rep_name || (repId ? (repNameById.get(repId) || "Unknown") : "Unassigned"),
        properties: new Map(),
      });
    }
    const rg = repGroups.get(repId);

    const propId = row.property_id || "";
    if (!rg.properties.has(propId)) {
      rg.properties.set(propId, {
        property_id: propId,
        property_name: row.property_name || "Unknown",
        address_line: row.address_line || "",
        units: [],
      });
    }
    rg.properties.get(propId).units.push({
      unit_number: row.unit_number,
      unit_key: row.unit_key,
      event_type: row.event_type,
    });
  }

  // Convert to arrays and sort
  const repsArr = Array.from(repGroups.values()).map((rg) => {
    const propsArr = Array.from(rg.properties.values()).map((p) => {
      const units = (p.units || []).sort((a, b) =>
        safeStr(a.unit_number).localeCompare(safeStr(b.unit_number))
      );
      return { ...p, units, unit_count: units.length };
    });

    propsArr.sort((a, b) => b.unit_count - a.unit_count || a.property_name.localeCompare(b.property_name));

    const totalUnits = propsArr.reduce((sum, p) => sum + (p.unit_count || 0), 0);

    return {
      rep_id: rg.rep_id,
      rep_name: rg.rep_name,
      properties: propsArr,
      total_units: totalUnits,
    };
  });

  repsArr.sort((a, b) => b.total_units - a.total_units || a.rep_name.localeCompare(b.rep_name));
  return repsArr;
}

/**
 * Build "Last 7 Days" groups: Property -> [units...]
 * Dedup by property_id + unit_key (fallback unit_number)
 */
function buildLast7ByProperty({ dailyRows, properties }) {
  const propById = new Map((properties || []).map((p) => [p.id, p]));

  const dedup = new Map(); // key -> row
  for (const r of dailyRows || []) {
    const propertyId = r.property_id || "";
    const unitKey = safeStr(r.unit_key).trim();
    const unitNumber = safeStr(r.unit_number).trim();

    const dedupKey = `${propertyId}::${unitKey || unitNumber || "unknown"}`;
    if (dedup.has(dedupKey)) continue;

    const p = propById.get(propertyId) || null;

    dedup.set(dedupKey, {
      property_id: propertyId,
      property_name: (p && p.name) || r.property_name || "Unknown",
      address_line: p ? formatAddressLine(p) : "",
      unit_number: unitNumber || "",
      unit_key: unitKey || "",
      event_date: r.event_date || "",
      event_type: r.event_type || "",
      source: r.source || "",
    });
  }

  const groups = new Map(); // property_id -> {property... units: []}
  for (const row of dedup.values()) {
    const pid = row.property_id || "";
    if (!groups.has(pid)) {
      groups.set(pid, {
        property_id: pid,
        property_name: row.property_name,
        address_line: row.address_line,
        units: [],
      });
    }
    groups.get(pid).units.push({
      unit_number: row.unit_number,
      unit_key: row.unit_key,
      first_seen: row.event_date,
      event_type: row.event_type,
    });
  }

  const arr = Array.from(groups.values()).map((g) => {
    const units = (g.units || []).sort((a, b) =>
      safeStr(a.unit_number).localeCompare(safeStr(b.unit_number))
    );
    return { ...g, units, unit_count: units.length };
  });

  arr.sort((a, b) => b.unit_count - a.unit_count || a.property_name.localeCompare(b.property_name));
  return arr;
}

function leadKeyForUnit({ property_id, unit_key, unit_number }) {
  const uk = safeStr(unit_key).trim();
  const un = safeStr(unit_number).trim();
  return `${property_id}::${uk || un || "unknown"}`;
}

export default function App() {
  // ---------------------------
  // App mode (Phase 1): Admin with "View as rep"
  // ---------------------------
  const [isAdminMode, setIsAdminMode] = useState(true);
  const [isAdminUser, setIsAdminUser] = useState(false);
  const [adminViewRepId, setAdminViewRepId] = useState(""); // "" => all
  // effective rep scope (Phase 1)
  const effectiveRepId = isAdminMode ? adminViewRepId : adminViewRepId;

  const [tab, setTab] = useState("callQueue"); // callQueue | last7 | crm | dashboard | properties | reps | dailyUnits | currentUnits
  const [status, setStatus] = useState("");

  // Shared data
  const [properties, setProperties] = useState([]);
  const [reps, setReps] = useState([]);
  const [loadingProps, setLoadingProps] = useState(true);
  const [loadingReps, setLoadingReps] = useState(true);

  // Add property form
  const [pName, setPName] = useState("");
  const [pUrl, setPUrl] = useState("");
  const [pPlatform, setPPlatform] = useState("");
  const [pAddress, setPAddress] = useState("");
  const [pCity, setPCity] = useState("");
  const [pZip, setPZip] = useState("");

  // Property edit state
  const [editingPropId, setEditingPropId] = useState(null);
  const [editName, setEditName] = useState("");
  const [editUrl, setEditUrl] = useState("");
  const [editPlatform, setEditPlatform] = useState("");
  const [editAddress, setEditAddress] = useState("");
  const [editCity, setEditCity] = useState("");
  const [editZip, setEditZip] = useState("");

  // Reps form
  const [repName, setRepName] = useState("");
  const [editingRepId, setEditingRepId] = useState(null);
  const [editingRepName, setEditingRepName] = useState("");

  // Daily Units (changes) state
  const [dailyRows, setDailyRows] = useState([]);
  const [dailyLoading, setDailyLoading] = useState(false);
  const [dailyDate, setDailyDate] = useState(isoDateLocal(new Date()));
  const [dailySource, setDailySource] = useState("all");
  const [dailyEventType, setDailyEventType] = useState("appeared");

  // Move-Out Call Queue state (built from daily_units)
  const [cqDate, setCqDate] = useState(isoDateLocal(new Date()));
  const [cqPropertyId, setCqPropertyId] = useState("");
  const [cqLoading, setCqLoading] = useState(false);
  const [cqGroups, setCqGroups] = useState([]);

  // Last 7 Days
  const [last7EndDate, setLast7EndDate] = useState(isoDateLocal(new Date()));
  const [last7Loading, setLast7Loading] = useState(false);
  const [last7PropertyId, setLast7PropertyId] = useState("");
  const [last7Groups, setLast7Groups] = useState([]);

  // Current Units (On Market) - SNAPSHOT view
  const [currentDate, setCurrentDate] = useState(isoDateLocal(new Date()));
  const [currentCity, setCurrentCity] = useState("");
  const [currentZip, setCurrentZip] = useState("");
  const [currentPropertyId, setCurrentPropertyId] = useState("");
  const [currentSortBy, setCurrentSortBy] = useState("property"); // property | rep
  const [currentLoading, setCurrentLoading] = useState(false);
  const [currentGroups, setCurrentGroups] = useState([]);

  // Dashboard
  const [dashDate, setDashDate] = useState(isoDateLocal(new Date()));
  const [dashCity, setDashCity] = useState("");
  const [dashZip, setDashZip] = useState("");
  const [dashRepFilter, setDashRepFilter] = useState(""); // "" => all reps
  const [dashLoading, setDashLoading] = useState(false);
  const [dashRows, setDashRows] = useState([]);
  const [dashTotals, setDashTotals] = useState({
    totalProperties: 0,
    totalReps: 0,
    totalUnitsOnMarket: 0,
  });
  const [dashLeadSeries, setDashLeadSeries] = useState([]); // [{date,count}]
  const [dashChartMetric, setDashChartMetric] = useState("appeared"); // "appeared" | "leads"

  // CRM Leads
  const [leads, setLeads] = useState([]);
  const [leadsLoading, setLeadsLoading] = useState(false);
  const [showCrmCalendar, setShowCrmCalendar] = useState(false);
  const [crmDateFilter, setCrmDateFilter] = useState(null); // YYYY-MM-DD
  const [crmDateFilterKind, setCrmDateFilterKind] = useState("all"); // all | new | fu
  const [crmCalendarMonth, setCrmCalendarMonth] = useState(() => new Date());
  const [crmDayModal, setCrmDayModal] = useState(null); // {dateISO, newLeads, followups}
  const [leadFollowUpAt, setLeadFollowUpAt] = useState(""); // datetime-local
  const [leadFollowUpMethods, setLeadFollowUpMethods] = useState({ email: false, call: true, text: false, visit: false });
  const [leadStatusFilter, setLeadStatusFilter] = useState("all"); // all | new | called | reached | follow_up | closed | lost
  const [selectedLead, setSelectedLead] = useState(null);

  const platformValuesSet = useMemo(() => new Set(PLATFORM_OPTIONS.map((o) => o.value)), []);
  const repMap = useMemo(() => new Map(reps.map((r) => [r.id, r.name])), [reps]);

  // Rep-scoped properties (Phase 1)
  const scopedProperties = useMemo(() => {
    if (!effectiveRepId) return properties;
    return properties.filter((p) => (p.rep_id ?? "") === effectiveRepId);
  }, [properties, effectiveRepId]);

  const cityOptions = useMemo(() => uniqSorted(scopedProperties.map((p) => p.city)), [scopedProperties]);
  const zipOptions = useMemo(() => uniqSorted(scopedProperties.map((p) => p.zip)), [scopedProperties]);

  // ---------------------------
  // Loaders
  // ---------------------------
  const loadReps = async () => {
    setLoadingReps(true);
    const { data, error } = await supabase.from("reps").select("id, name, created_at").order("name", { ascending: true });
    if (error) {
      console.error("loadReps error:", error);
      setStatus(`❌ Error loading reps: ${error.message}`);
      setLoadingReps(false);
      return;
    }
    setReps(data || []);

    const repRows = data || [];
    // If RLS only returns a single rep row, assume this is a Rep login.
    // If multiple rep rows are visible, assume Admin.
    setIsAdminUser(repRows.length > 1);
    if (repRows.length === 1 && repRows[0]?.id) {
      setAdminViewRepId(repRows[0].id);
      setIsAdminMode(false);
    }
    setLoadingReps(false);
  };

  const loadProperties = async () => {
    setLoadingProps(true);
    const { data, error } = await supabase.from("properties").select("*").order("created_at", { ascending: false });
    if (error) {
      console.error("loadProperties error:", error);
      setStatus(`❌ Error loading properties: ${error.message}`);
      setLoadingProps(false);
      return;
    }
    setProperties(data || []);
    setLoadingProps(false);
  };

  useEffect(() => {
    (async () => {
      await loadReps();
      await loadProperties();
    })();
  }, []);

  // Ensure Rep logins can’t access Admin-only controls/mode.
  useEffect(() => {
    if (!isAdminUser) {
      setIsAdminMode(false);
    }
  }, [isAdminUser]);


  // ---------------------------
  // Daily Units Loader (EVENTS) (table: daily_units)
  // ---------------------------
  const loadDailyUnits = async () => {
    try {
      setDailyLoading(true);

      let q = supabase
        .from("daily_units")
        .select("event_date, property_id, property_name, rep_id, unit_key, unit_number, event_type, source")
        .eq("event_date", dailyDate)
        .eq("event_type", dailyEventType)
        .order("property_name", { ascending: true })
        .order("unit_number", { ascending: true });

      // In rep-scoped mode, we filter by assigned properties (stronger than daily_units.rep_id)
      if (effectiveRepId) {
        const ids = scopedProperties.map((p) => p.id);
        if (ids.length > 0) q = q.in("property_id", ids);
        else {
          setDailyRows([]);
          setDailyLoading(false);
          return;
        }
      }

      if (dailySource && dailySource !== "all") q = q.eq("source", dailySource);

      const { data, error } = await q;
      if (error) {
        console.error("loadDailyUnits error:", error);
        setStatus(`❌ Daily units error: ${error.message}`);
        setDailyRows([]);
        setDailyLoading(false);
        return;
      }

      setDailyRows(data || []);
      setDailyLoading(false);
    } catch (err) {
      console.error(err);
      setStatus(`❌ Daily units error: ${err.message || "unknown error"}`);
      setDailyRows([]);
      setDailyLoading(false);
    }
  };

  useEffect(() => {
    if (tab !== "dailyUnits") return;
    if (loadingProps) return;
    loadDailyUnits();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tab, dailyDate, dailySource, dailyEventType, effectiveRepId, scopedProperties.length]);

  // ---------------------------
  // Move-Out Call Queue Loader (table: daily_units)
  // ---------------------------
  const loadCallQueue = async () => {
    try {
      setCqLoading(true);
      setStatus(`Loading call queue for ${cqDate}...`);

      let q = supabase
        .from("daily_units")
        .select("event_date, property_id, property_name, rep_id, unit_key, unit_number, event_type, source")
        .eq("event_date", cqDate)
        .eq("event_type", "appeared")
        .order("property_name", { ascending: true })
        .order("unit_number", { ascending: true });

      if (cqPropertyId) q = q.eq("property_id", cqPropertyId);

      // Rep scope = assigned properties
      if (effectiveRepId) {
        const ids = scopedProperties.map((p) => p.id);
        if (ids.length > 0) q = q.in("property_id", ids);
        else {
          setCqGroups([]);
          setCqLoading(false);
          return;
        }
      }

      const { data, error } = await q;
      if (error) {
        console.error("loadCallQueue error:", error);
        setStatus(`❌ Call queue error: ${error.message}`);
        setCqGroups([]);
        setCqLoading(false);
        return;
      }

      // Build groups (if rep-scoped, this will typically return 1 rep group or unassigned)
      const groups = buildCallQueue({ dailyRows: data || [], properties, reps });

      // If rep-scoped, filter to that rep id (by assignment rule)
      const filteredGroups = effectiveRepId ? groups.filter((g) => (g.rep_id ?? "") === effectiveRepId) : groups;

      setCqGroups(filteredGroups);
      setCqLoading(false);
      setStatus(`✅ Call queue loaded for ${cqDate}.`);
    } catch (err) {
      console.error("loadCallQueue error:", err);
      setStatus(`❌ Call queue error: ${err.message || "unknown error"}`);
      setCqGroups([]);
      setCqLoading(false);
    }
  };

  useEffect(() => {
    if (tab !== "callQueue") return;
    if (loadingProps || loadingReps) return;
    loadCallQueue();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tab, properties, reps, cqDate, cqPropertyId, effectiveRepId, scopedProperties.length]);

  // ---------------------------
  // Last 7 Days Loader (table: daily_units)
  // ---------------------------
  const loadLast7 = async () => {
    try {
      setLast7Loading(true);
      setStatus("Loading last 7 days...");

      const startISO = addDaysISO(last7EndDate, -6);

      let q = supabase
        .from("daily_units")
        .select("event_date, property_id, property_name, rep_id, unit_key, unit_number, event_type, source")
        .gte("event_date", startISO)
        .lte("event_date", last7EndDate)
        .eq("event_type", "appeared")
        .order("property_name", { ascending: true })
        .order("unit_number", { ascending: true });

      if (last7PropertyId) q = q.eq("property_id", last7PropertyId);

      // Rep scope via assigned properties
      if (effectiveRepId) {
        const ids = scopedProperties.map((p) => p.id);
        if (ids.length > 0) q = q.in("property_id", ids);
        else {
          setLast7Groups([]);
          setLast7Loading(false);
          return;
        }
      }

      const { data, error } = await q;
      if (error) {
        console.error("loadLast7 error:", error);
        setStatus(`❌ Last 7 days error: ${error.message}`);
        setLast7Groups([]);
        setLast7Loading(false);
        return;
      }

      const groups = buildLast7ByProperty({ dailyRows: data || [], properties });
      setLast7Groups(groups);
      setLast7Loading(false);
      setStatus(`✅ Last 7 days loaded (${startISO} → ${last7EndDate}).`);
    } catch (err) {
      console.error("loadLast7 error:", err);
      setStatus(`❌ Last 7 days error: ${err.message || "unknown error"}`);
      setLast7Groups([]);
      setLast7Loading(false);
    }
  };

  useEffect(() => {
    if (tab !== "last7") return;
    if (loadingProps) return;
    loadLast7();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tab, last7EndDate, last7PropertyId, effectiveRepId, scopedProperties.length]);

  // ---------------------------
  // Current Units Loader (SNAPSHOTS)
  // ---------------------------
  const loadCurrentUnits = async () => {
    try {
      setCurrentLoading(true);
      setStatus(`Loading current units for ${currentDate}...`);

      const filteredProps = scopedProperties.filter((p) => {
        if (currentPropertyId && p.id !== currentPropertyId) return false;
        if (currentCity && safeStr(p.city).trim() !== currentCity) return false;
        if (currentZip && safeStr(p.zip).trim() !== currentZip) return false;
        return true;
      });

      const propIds = filteredProps.map((p) => p.id);
      const latestMap = await fetchLatestSnapshotsByProperty({ propertyIds: propIds, dateISO: currentDate });

      const groups = [];
      for (const p of filteredProps) {
        const snap = latestMap.get(p.id);
        if (!snap) continue;

        const units = Array.isArray(snap.units_json) ? snap.units_json : [];
        const unitRows = units
          .map((u) => ({
            unit_number: extractUnitNumber(u),
            unit_key: extractUnitKey(u),
          }))
          .sort((a, b) => safeStr(a.unit_number).localeCompare(safeStr(b.unit_number)));

        groups.push({
          property_id: p.id,
          property_name: p.name,
          address_line: formatAddressLine(p),
          rep_id: p.rep_id ?? "",
          rep_name: p.rep_id ? (repMap.get(p.rep_id) || "Unknown") : "Unassigned",
          snapshot_date: snap.snapshot_date,
          from_date: snap.snapshot_date === currentDate ? "exact" : "fallback",
          count: unitRows.length,
          rows: unitRows,
        });
      }

      if (currentSortBy === "rep") {
        groups.sort((a, b) => a.rep_name.localeCompare(b.rep_name) || a.property_name.localeCompare(b.property_name));
      } else {
        groups.sort((a, b) => a.property_name.localeCompare(b.property_name));
      }

      setCurrentGroups(groups);
      setCurrentLoading(false);
      setStatus(`✅ Loaded current units for ${currentDate}.`);
    } catch (err) {
      console.error("loadCurrentUnits error:", err);
      setStatus(`❌ Current units error: ${err.message || "unknown error"}`);
      setCurrentGroups([]);
      setCurrentLoading(false);
    }
  };

  useEffect(() => {
    if (tab !== "currentUnits") return;
    if (loadingProps) return;
    loadCurrentUnits();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tab, scopedProperties, currentDate, currentCity, currentZip, currentPropertyId, currentSortBy]);

  // ---------------------------
  // Dashboard Loader
  // ---------------------------
  const loadDashboard = async () => {
    try {
      setDashLoading(true);
      setStatus(`Loading dashboard for ${dashDate}...`);

      // filters apply on TOP of rep scope (View-as-Rep)
      const filteredProps = scopedProperties.filter((p) => {
        if (dashCity && safeStr(p.city).trim() !== dashCity) return false;
        if (dashZip && safeStr(p.zip).trim() !== dashZip) return false;
        if (dashRepFilter && safeStr(p.rep_id).trim() !== dashRepFilter) return false;
        return true;
      });

      const propIds = filteredProps.map((p) => p.id);
      const latestMap = await fetchLatestSnapshotsByProperty({ propertyIds: propIds, dateISO: dashDate });

      const rows = filteredProps.map((p) => {
        const snap = latestMap.get(p.id);
        const units = Array.isArray(snap?.units_json) ? snap.units_json : [];
        return {
          property_id: p.id,
          property_name: p.name,
          address_line: formatAddressLine(p),
          city: p.city ?? "",
          zip: p.zip ?? "",
          rep_id: p.rep_id ?? "",
          rep_name: p.rep_id ? (repMap.get(p.rep_id) || "Unknown") : "Unassigned",
          units_on_market: units.length,
          snapshot_date: snap?.snapshot_date ?? "",
          snapshot_note: snap?.snapshot_date === dashDate ? "exact" : (snap ? "fallback" : "none"),
        };
      });

      // Group by rep for display
      const repGroupsMap = new Map(); // rep_id -> {rep_id, rep_name, total_units, rows: []}
      for (const r of rows) {
        const rid = r.rep_id || "";
        if (!repGroupsMap.has(rid)) {
          repGroupsMap.set(rid, { rep_id: rid, rep_name: r.rep_name, total_units: 0, rows: [] });
        }
        const g = repGroupsMap.get(rid);
        g.rows.push(r);
        g.total_units += r.units_on_market || 0;
      }

      const repGroups = Array.from(repGroupsMap.values()).map((g) => {
        g.rows.sort((a, b) => b.units_on_market - a.units_on_market || a.property_name.localeCompare(b.property_name));
        return g;
      });
      repGroups.sort((a, b) => b.total_units - a.total_units || a.rep_name.localeCompare(b.rep_name));

      // We'll store rep-groups in dashRows (so render can show grouped sections)
      setDashRows(repGroups);

      const totalUnits = rows.reduce((sum, r) => sum + (r.units_on_market || 0), 0);
      setDashTotals({
        totalProperties: properties.length,
        totalReps: reps.length,
        totalUnitsOnMarket: totalUnits,
      });

      // Last 7 days bar series (metric toggle):
      // - "appeared" = new units appearing in daily_units (move-out signals)
      // - "leads"    = CRM leads created in leads table
      const startISO = addDaysISO(dashDate, -6);

      // seed 7 days
      const counts = new Map();
      for (let i = 0; i < 7; i++) {
        const d = addDaysISO(startISO, i);
        counts.set(d, 0);
      }

      if (dashChartMetric === "leads") {
        const startTS = `${startISO}T00:00:00`;
        const endTS = `${dashDate}T23:59:59`;

        let lq = supabase.from("leads").select("created_at, rep_id");
        lq = lq.gte("created_at", startTS).lte("created_at", endTS);
        if (effectiveRepId) lq = lq.eq("rep_id", effectiveRepId);
        if (dashRepFilter) lq = lq.eq("rep_id", dashRepFilter);

        const { data: leadRows, error: leadErr } = await lq;
        if (leadErr) {
          console.error("loadDashboard leads error:", leadErr);
          setDashLeadSeries([]);
        } else {
          for (const lr of leadRows || []) {
            const d = (lr.created_at || "").slice(0, 10);
            if (counts.has(d)) counts.set(d, (counts.get(d) || 0) + 1);
          }
          const series = Array.from(counts.entries()).map(([date, count]) => ({ date, count }));
          setDashLeadSeries(series);
        }
      } else {
        // default: daily_units appeared
        // IMPORTANT: daily_units event_date is YYYY-MM-DD so we count by that.
        // We also dedupe by (event_date + property_id + unit_key/fallback unit_number) to avoid duplicates.
        let dq = supabase
          .from("daily_units")
          .select("event_date, property_id, unit_key, unit_number")
          .gte("event_date", startISO)
          .lte("event_date", dashDate)
          .eq("event_type", "appeared");

        // apply scope filters using the same filtered property list
        if (propIds.length > 0) dq = dq.in("property_id", propIds);
        else {
          setDashLeadSeries(Array.from(counts.entries()).map(([date, count]) => ({ date, count })));
          setDashLoading(false);
          setStatus(`✅ Dashboard loaded for ${dashDate}.`);
          return;
        }

        const { data: drows, error: derr } = await dq;
        if (derr) {
          console.error("loadDashboard daily_units error:", derr);
          setDashLeadSeries([]);
        } else {
          const seen = new Set();
          for (const r of drows || []) {
            const date = r.event_date;
            const key = `${date}::${r.property_id}::${(r.unit_key || r.unit_number || "unknown")}`;
            if (seen.has(key)) continue;
            seen.add(key);
            if (counts.has(date)) counts.set(date, (counts.get(date) || 0) + 1);
          }
          const series = Array.from(counts.entries()).map(([date, count]) => ({ date, count }));
          setDashLeadSeries(series);
        }
      }

      setDashLoading(false);
      setStatus(`✅ Dashboard loaded for ${dashDate}.`);
    } catch (err) {
      console.error("loadDashboard error:", err);
      setStatus(`❌ Dashboard error: ${err.message || "unknown error"}`);
      setDashRows([]);
      setDashLeadSeries([]);
      setDashTotals({
        totalProperties: properties.length,
        totalReps: reps.length,
        totalUnitsOnMarket: 0,
      });
      setDashLoading(false);
    }
  };

  useEffect(() => {
    if (tab !== "dashboard") return;
    if (loadingProps || loadingReps) return;
    loadDashboard();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tab, scopedProperties, dashDate, dashCity, dashZip, dashRepFilter, dashChartMetric]);

  // ---------------------------
  // CRM: Load leads
  // ---------------------------
  const loadLeads = async () => {
    try {
      setLeadsLoading(true);

      let q = supabase.from("leads").select("*").order("updated_at", { ascending: false });

      if (effectiveRepId) q = q.eq("rep_id", effectiveRepId);

      if (leadStatusFilter !== "all") q = q.eq("status", leadStatusFilter);

      const { data, error } = await q;
      if (error) {
        console.error("loadLeads error:", error);
        setStatus(`❌ CRM leads error: ${error.message}`);
        setLeads([]);
        setLeadsLoading(false);
        return;
      }

      setLeads(data || []);
      setLeadsLoading(false);
    } catch (err) {
      console.error("loadLeads error:", err);
      setStatus(`❌ CRM leads error: ${err.message || "unknown error"}`);
      setLeads([]);
      setLeadsLoading(false);
    }
  };

  const openLead = (lead) => {
    setSelectedLead(lead);

    if (lead?.follow_up_at) {
      const fu = new Date(lead.follow_up_at);
      const local = new Date(fu.getTime() - fu.getTimezoneOffset() * 60000).toISOString().slice(0, 16);
      setLeadFollowUpAt(local);
    } else {
      setLeadFollowUpAt("");
    }

    const methods = (lead?.follow_up_methods || "")
      .split(",")
      .map((s) => s.trim().toLowerCase())
      .filter(Boolean);

    setLeadFollowUpMethods({
      email: methods.includes("email"),
      call: methods.includes("call"),
      text: methods.includes("text"),
      visit: methods.includes("visit"),
    });
  };

  const saveLeadFollowUp = async () => {
    if (!selectedLead) return;
    try {
      const methods = Object.entries(leadFollowUpMethods)
        .filter(([_, v]) => v)
        .map(([k]) => k)
        .join(",");

      const followUpAtISO = leadFollowUpAt ? new Date(leadFollowUpAt).toISOString() : null;

      const { error } = await supabase
        .from("leads")
        .update({
          follow_up_at: followUpAtISO,
          follow_up_methods: methods || null,
          status: selectedLead.status === "new" && followUpAtISO ? "follow_up" : selectedLead.status,
        })
        .eq("id", selectedLead.id);

      if (error) throw error;

      setStatus("✅ Follow-up saved.");
      await loadLeads();
    } catch (err) {
      console.error("saveLeadFollowUp error:", err);
      setStatus(`❌ Save follow-up error: ${err.message || "unknown error"}`);
    }
  };


  useEffect(() => {
    if (tab !== "crm") return;
    loadLeads();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tab, effectiveRepId, leadStatusFilter]);

  const upsertLeadFromUnit = async ({ property_id, property_name, address_line, unit_number, unit_key }) => {
    if (!effectiveRepId) {
      alert("Select a rep first (Admin: use 'View as Rep').");
      return;
    }

    const lk = leadKeyForUnit({ property_id, unit_key, unit_number });
    const payload = {
      rep_id: effectiveRepId,
      lead_key: lk,
      property_id,
      property_name,
      address_line,
      unit_number: unit_number || null,
      unit_key: unit_key || null,
      status: "new",
      notes: "",
      last_contacted_at: null,
    };

    const { error } = await supabase
      .from("leads")
      .upsert(payload, { onConflict: "rep_id,lead_key" });

    if (error) {
      console.error(error);
      alert("Could not create lead: " + error.message);
      return;
    }

    setStatus("✅ Lead created/updated.");
    await loadLeads();
    setTab("crm");
  };

  const updateLead = async (leadId, patch) => {
    const { error } = await supabase.from("leads").update(patch).eq("id", leadId);
    if (error) {
      console.error(error);
      alert(error.message);
      return;
    }
    await loadLeads();
  };

  // ---------------------------
  // Properties CRUD
  // ---------------------------
  const addProperty = async (e) => {
    e.preventDefault();
    if (!pPlatform) {
      setStatus("❌ Please select a platform.");
      return;
    }

    setStatus("Saving property...");

    const { error } = await supabase.from("properties").insert([
      {
        name: pName,
        url: pUrl,
        platform: normalizePlatformValue(pPlatform),
        address: pAddress,
        city: pCity,
        zip: pZip,
      },
    ]);

    if (error) {
      console.error(error);
      setStatus(`❌ Error saving property: ${error.message}`);
      return;
    }

    setStatus("✅ Property saved!");
    setPName("");
    setPUrl("");
    setPPlatform("");
    setPAddress("");
    setPCity("");
    setPZip("");
    await loadProperties();
  };

  const deleteProperty = async (id) => {
    if (!confirm("Delete this property?")) return;

    const { error } = await supabase.from("properties").delete().eq("id", id);
    if (error) {
      console.error(error);
      alert(error.message);
      return;
    }
    await loadProperties();
  };

  const assignRep = async (propertyId, repId) => {
    const repValue = repId === "" ? null : repId;

    const { error } = await supabase.from("properties").update({ rep_id: repValue }).eq("id", propertyId);

    if (error) {
      console.error(error);
      alert(error.message);
      return;
    }

    setProperties((prev) => prev.map((p) => (p.id === propertyId ? { ...p, rep_id: repValue } : p)));
  };

  const startEditProperty = (p) => {
    setEditingPropId(p.id);
    setEditName(p.name || "");
    setEditUrl(p.url || "");
    setEditPlatform(normalizePlatformValue(p.platform || "unknown"));
    setEditAddress(p.address || "");
    setEditCity(p.city || "");
    setEditZip(p.zip || "");
  };

  const cancelEditProperty = () => {
    setEditingPropId(null);
    setEditName("");
    setEditUrl("");
    setEditPlatform("");
    setEditAddress("");
    setEditCity("");
    setEditZip("");
  };

  const saveEditProperty = async () => {
    if (!editingPropId) return;
    if (!editPlatform) {
      alert("Please select a platform.");
      return;
    }

    setStatus("Updating property...");

    const { error } = await supabase
      .from("properties")
      .update({
        name: editName,
        url: editUrl,
        platform: normalizePlatformValue(editPlatform),
        address: editAddress,
        city: editCity,
        zip: editZip,
      })
      .eq("id", editingPropId);

    if (error) {
      console.error(error);
      setStatus(`❌ Error updating property: ${error.message}`);
      alert(error.message);
      return;
    }

    setStatus("✅ Property updated!");
    cancelEditProperty();
    await loadProperties();
  };

  // ---------------------------
  // Reps CRUD
  // ---------------------------
  const addRep = async (e) => {
    e.preventDefault();
    setStatus("Saving rep...");

    const { error } = await supabase.from("reps").insert([{ name: repName }]);
    if (error) {
      console.error(error);
      setStatus(`❌ Error saving rep: ${error.message}`);
      return;
    }

    setStatus("✅ Rep saved!");
    setRepName("");
    await loadReps();
  };

  const startEditRep = (rep) => {
    setEditingRepId(rep.id);
    setEditingRepName(rep.name);
  };

  const cancelEditRep = () => {
    setEditingRepId(null);
    setEditingRepName("");
  };

  const saveEditRep = async () => {
    if (!editingRepId) return;
    setStatus("Updating rep...");

    const { error } = await supabase.from("reps").update({ name: editingRepName }).eq("id", editingRepId);

    if (error) {
      console.error(error);
      setStatus(`❌ Error updating rep: ${error.message}`);
      return;
    }

    setStatus("✅ Rep updated!");
    cancelEditRep();
    await loadReps();
  };

  const deleteRep = async (repId) => {
    if (!confirm("Delete this rep? (Properties assigned to this rep will become Unassigned)")) return;

    const { error: unassignError } = await supabase.from("properties").update({ rep_id: null }).eq("rep_id", repId);

    if (unassignError) {
      console.error(unassignError);
      alert("Could not unassign properties: " + unassignError.message);
      return;
    }

    const { error } = await supabase.from("reps").delete().eq("id", repId);
    if (error) {
      console.error(error);
      alert(error.message);
      return;
    }

    await loadReps();
    await loadProperties();
  };

  // ---------------------------
  // UI helpers
  // ---------------------------
  const currentRepName = effectiveRepId ? (repMap.get(effectiveRepId) || "Unknown") : "All";
  const scopedPropsCount = scopedProperties.length;

  const handleLogout = async () => {
    try {
      await supabase.auth.signOut();
    } catch (e) {
      console.error("Logout failed:", e);
    }
  };

  return (
    <div style={{ padding: 20, fontFamily: "Arial", background: ui.bg, minHeight: "100vh" }}>
      <div style={ui.header}>
        <div>
          <div style={{ fontSize: 18, fontWeight: "bold" }}>Unit Tracker</div>
          <div style={{ fontSize: 12, color: "#cbd5e1" }}>Move-Out Call Queue + CRM</div>
        </div>

        <div style={{ display: "flex", gap: 12, alignItems: "center" }}>
          <div style={ui.pill}>
            <span style={{ color: "#94a3b8", fontSize: 12 }}>Scope</span>
            <div style={{ fontWeight: "bold" }}>{isAdminMode ? "Admin" : "Rep"}</div>
          </div>

          {isAdminUser && (
          <>
          {/* Phase 1: Admin "View as Rep" */}
          <div style={ui.pill}>
            <span style={{ color: "#94a3b8", fontSize: 12 }}>View as Rep</span>
            <select value={adminViewRepId} onChange={(e) => setAdminViewRepId(e.target.value)} style={ui.selectDark}>
              <option value="">All</option>
              {reps.map((r) => (
                <option key={r.id} value={r.id}>
                  {r.name}
                </option>
              ))}
            </select>
          </div>

          <label style={{ color: "white", fontSize: 12, display: "flex", gap: 8, alignItems: "center" }}>
            <input type="checkbox" checked={isAdminMode} onChange={(e) => setIsAdminMode(e.target.checked)} />
            Admin Mode
          </label>
          </>
          )}

          <div style={{ color: "#e2e8f0", fontSize: 12, maxWidth: 420, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
            {status}
          </div>
        

          <button onClick={handleLogout} style={ui.logoutBtn}>
            Logout
          </button>
</div>
      </div>

      {/* Tabs */}
      <div style={ui.tabs}>
        <button onClick={() => setTab("callQueue")} style={tabBtn(tab === "callQueue")}>Move-Out Call Queue</button>
        <button onClick={() => setTab("last7")} style={tabBtn(tab === "last7")}>Last 7 Days</button>
        <button onClick={() => setTab("crm")} style={tabBtn(tab === "crm")}>CRM Leads</button>
        <button onClick={() => setTab("dashboard")} style={tabBtn(tab === "dashboard")}>Dashboard</button>
        <button onClick={() => setTab("currentUnits")} style={tabBtn(tab === "currentUnits")}>Current Units</button>
        <button onClick={() => setTab("dailyUnits")} style={tabBtn(tab === "dailyUnits")}>Daily Units</button>
        <button onClick={() => setTab("properties")} style={tabBtn(tab === "properties")}>Properties</button>
        {isAdminUser && (
        <button onClick={() => setTab("reps")} style={tabBtn(tab === "reps")}>Reps</button>
        )}

        <div style={{ marginLeft: "auto", color: "#334155", alignSelf: "center", fontSize: 12 }}>
          <b>Rep:</b> {currentRepName} • <b>Props:</b> {scopedPropsCount}
        </div>
      </div>

      {/* ---------------- Move-Out Call Queue ---------------- */}
      {tab === "callQueue" ? (
        <>
          <div style={card}>
            <h2 style={{ marginTop: 0 }}>Move-Out Call Queue</h2>
            <div style={{ marginBottom: 8, color: "#64748b" }}>
              Rule: <b>daily_units.event_type = 'appeared'</b> → move-out signal.
            </div>

            <div style={{ display: "flex", gap: 12, flexWrap: "wrap", alignItems: "center" }}>
              <label>
                Date:&nbsp;
                <input type="date" value={cqDate} onChange={(e) => setCqDate(e.target.value)} style={ui.input} />
              </label>

              <label>
                Property:&nbsp;
                <select value={cqPropertyId} onChange={(e) => setCqPropertyId(e.target.value)} style={ui.select}>
                  <option value="">All</option>
                  {scopedProperties.map((p) => (
                    <option key={p.id} value={p.id}>{p.name}</option>
                  ))}
                </select>
              </label>

              <button onClick={loadCallQueue} style={ui.primaryBtn}>Refresh</button>
            </div>

            <div style={{ marginTop: 10, color: "#475569" }}>
              Reps: <b>{cqGroups.length}</b> • Properties:{" "}
              <b>{cqGroups.reduce((s, g) => s + (g.properties?.length || 0), 0)}</b> • Units:{" "}
              <b>{cqGroups.reduce((s, g) => s + (g.total_units || 0), 0)}</b>
              {cqLoading ? " (loading...)" : ""}
            </div>
          </div>

          <div style={{ marginTop: 16 }}>
            {cqLoading ? (
              <p>Loading...</p>
            ) : cqGroups.length === 0 ? (
              <p style={{ color: "#64748b" }}>No new units found for this date.</p>
            ) : (
              <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
                {cqGroups.map((rg) => (
                  <div key={rg.rep_id || "__unassigned__"} style={card}>
                    <h3 style={{ margin: 0 }}>
                      {rg.rep_name} <span style={badge("blue")}>{rg.total_units} new</span>
                    </h3>

                    <div style={{ display: "flex", flexDirection: "column", gap: 12, marginTop: 12 }}>
                      {rg.properties.map((p) => (
                        <div key={p.property_id} style={ui.innerCard}>
                          <div style={{ display: "flex", justifyContent: "space-between", gap: 12, flexWrap: "wrap" }}>
                            <div>
                              <div style={{ fontSize: 16, fontWeight: "bold" }}>
                                {p.property_name} <span style={{ color: "#475569", fontWeight: "normal" }}>({p.unit_count})</span>
                              </div>
                              <div style={{ marginTop: 4, color: "#64748b" }}>{p.address_line || "—"}</div>
                            </div>
                          </div>

                          <div style={{ overflowX: "auto", marginTop: 10 }}>
                            <table style={{ ...table, minWidth: 520 }}>
                              <thead>
                                <tr>
                                  <th style={th}>Unit #</th>
                                  <th style={th}>Event</th>
                                  <th style={th}>Lead</th>
                                </tr>
                              </thead>
                              <tbody>
                                {p.units.map((u, idx) => (
                                  <tr key={`${p.property_id}-${u.unit_key || u.unit_number}-${idx}`}>
                                    <td style={td}><b>{u.unit_number || ""}</b></td>
                                    <td style={td}>{u.event_type}</td>
                                    <td style={td}>
                                      <button type="button"
                                        style={ui.smallBtn}
                                        onClick={() =>
                                          upsertLeadFromUnit({
                                            property_id: p.property_id,
                                            property_name: p.property_name,
                                            address_line: p.address_line,
                                            unit_number: u.unit_number,
                                            unit_key: u.unit_key,
                                          })
                                        }
                                      >
                                        Create Lead
                                      </button>
                                    </td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </>
      ) : null}

      {/* ---------------- Last 7 Days ---------------- */}
      {tab === "last7" ? (
        <>
          <div style={card}>
            <h2 style={{ marginTop: 0 }}>Last 7 Days (New Units)</h2>

            <div style={{ marginBottom: 8, color: "#64748b" }}>
              This is for reps who work leads weekly. It shows <b>all units that appeared</b> in the last 7 days, grouped by property.
            </div>

            <div style={{ display: "flex", gap: 12, flexWrap: "wrap", alignItems: "center" }}>
              <label>
                End Date:&nbsp;
                <input type="date" value={last7EndDate} onChange={(e) => setLast7EndDate(e.target.value)} style={ui.input} />
              </label>

              <label>
                Property:&nbsp;
                <select value={last7PropertyId} onChange={(e) => setLast7PropertyId(e.target.value)} style={ui.select}>
                  <option value="">All</option>
                  {scopedProperties.map((p) => (
                    <option key={p.id} value={p.id}>{p.name}</option>
                  ))}
                </select>
              </label>

              <button onClick={loadLast7} style={ui.primaryBtn}>Refresh</button>

              <div style={{ color: "#475569" }}>
                Range: <b>{addDaysISO(last7EndDate, -6)}</b> → <b>{last7EndDate}</b>
              </div>
            </div>

            <div style={{ marginTop: 10, color: "#475569" }}>
              Properties: <b>{last7Groups.length}</b> • Units:{" "}
              <b>{last7Groups.reduce((s, g) => s + (g.unit_count || 0), 0)}</b>
              {last7Loading ? " (loading...)" : ""}
            </div>
          </div>

          <div style={{ marginTop: 16 }}>
            {last7Loading ? (
              <p>Loading...</p>
            ) : last7Groups.length === 0 ? (
              <p style={{ color: "#64748b" }}>No new units found in the last 7 days for this scope.</p>
            ) : (
              <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
                {last7Groups.map((g) => (
                  <div key={g.property_id} style={card}>
                    <div style={{ display: "flex", justifyContent: "space-between", gap: 12, flexWrap: "wrap" }}>
                      <div>
                        <h3 style={{ margin: 0 }}>
                          {g.property_name} <span style={badge("blue")}>{g.unit_count} units</span>
                        </h3>
                        <div style={{ marginTop: 4, color: "#64748b" }}>{g.address_line || "—"}</div>
                      </div>
                    </div>

                    <div style={{ overflowX: "auto", marginTop: 10 }}>
                      <table style={{ ...table, minWidth: 700 }}>
                        <thead>
                          <tr>
                            <th style={th}>Unit #</th>
                            <th style={th}>First Seen</th>
                            <th style={th}>Lead</th>
                          </tr>
                        </thead>
                        <tbody>
                          {g.units.map((u, idx) => (
                            <tr key={`${g.property_id}-${u.unit_key || u.unit_number}-${idx}`}>
                              <td style={td}><b>{u.unit_number || ""}</b></td>
                              <td style={td}>{u.first_seen}</td>
                              <td style={td}>
                                <button type="button"
                                  style={ui.smallBtn}
                                  onClick={() =>
                                    upsertLeadFromUnit({
                                      property_id: g.property_id,
                                      property_name: g.property_name,
                                      address_line: g.address_line,
                                      unit_number: u.unit_number,
                                      unit_key: u.unit_key,
                                    })
                                  }
                                >
                                  Create Lead
                                </button>
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>

                    <div style={{ marginTop: 10, color: "#64748b", fontSize: 12 }}>
                      Dedup: property_id + unit_key (fallback unit_number).
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </>
      ) : null}

      {/* ---------------- CRM ---------------- */}
      {tab === "crm" ? (
        <>
          <div style={card}>
            <h2 style={{ marginTop: 0 }}>CRM Leads</h2>

            <div style={{ display: "flex", gap: 12, flexWrap: "wrap", alignItems: "center" }}>
              <label>
                Status:&nbsp;
                <select value={leadStatusFilter} onChange={(e) => setLeadStatusFilter(e.target.value)} style={ui.select}>
                  <option value="all">All</option>
                  <option value="new">New</option>
                      <option value="called">Called</option>
                      <option value="reached">Reached</option>
                      <option value="follow_up">Follow Up</option>
                      <option value="closed">Closed</option>
                      <option value="lost">Lost</option></select>
              </label>

              <button onClick={loadLeads} style={ui.primaryBtn}>Refresh</button>
              <button onClick={() => {
                setShowCrmCalendar((v) => {
                  const next = !v;
                  if (next) {
                    // entering calendar
                  } else {
                    // entering list
                  }
                  return next;
                });
              }} style={ui.secondaryBtn}>
                {showCrmCalendar ? "List View" : "Calendar View"}
              </button>

              <div style={{ color: "#475569" }}>
                Showing <b>{leads.length}</b> leads {leadsLoading ? "(loading...)" : ""}
              </div>
            </div>

            <div style={{ marginTop: 10, color: "#64748b", fontSize: 12 }}>
              Note: CRM requires a <b>leads</b> table with a unique constraint on <b>(rep_id, lead_key)</b>.
            </div>
          </div>

          <div style={{ display: "grid", gridTemplateColumns: "1fr 360px", gap: 14, marginTop: 16 }}>
            <div style={card}>
              {leadsLoading ? (
                <p>Loading...</p>
              ) : showCrmCalendar ? (
                <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 10, flexWrap: "wrap" }}>
                    <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
                      <button type="button"
                        style={ui.smallBtn}
                        onClick={() => setCrmCalendarMonth(new Date(crmCalendarMonth.getFullYear(), crmCalendarMonth.getMonth() - 1, 1))}
                      >
                        ◀
                      </button>
                      <div style={{ fontWeight: 800, fontSize: 16 }}>{formatMonthTitle(crmCalendarMonth)}</div>
                      <button type="button"
                        style={ui.smallBtn}
                        onClick={() => setCrmCalendarMonth(new Date(crmCalendarMonth.getFullYear(), crmCalendarMonth.getMonth() + 1, 1))}
                      >
                        ▶
                      </button>
                    </div>

                    <div style={{ display: "flex", gap: 10, alignItems: "center", color: "#64748b", fontSize: 12 }}>
                      <span><span style={{ ...badge("blue"), padding: "2px 8px" }}>New</span> leads created</span>
                      <span><span style={{ ...badge("amber"), padding: "2px 8px" }}>FU</span> follow-ups scheduled</span>
                    </div>
                  </div>

                  <div style={{ display: "grid", gridTemplateColumns: "repeat(7, 1fr)", gap: 8 }}>
                    {["Sun","Mon","Tue","Wed","Thu","Fri","Sat"].map((w) => (
                      <div key={w} style={{ fontSize: 12, fontWeight: 700, color: "#475569", paddingLeft: 6 }}>{w}</div>
                    ))}

                    {(() => {
                      const days = getMonthGrid(crmCalendarMonth);
                      const monthStart = toISODateLocal(startOfMonth(crmCalendarMonth));
                      const monthEnd = toISODateLocal(endOfMonth(crmCalendarMonth));

                      const counts = new Map();
                      for (const d of days) counts.set(toISODateLocal(d), { newLeads: [], followups: [] });

                      for (const l of leads || []) {
                        const createdDay = (l.created_at || "").slice(0, 10);
                        if (createdDay && createdDay >= monthStart && createdDay <= monthEnd && counts.has(createdDay)) {
                          counts.get(createdDay).newLeads.push(l);
                        }
                        const fuDay = (l.follow_up_at || "").slice(0, 10);
                        if (fuDay && fuDay >= monthStart && fuDay <= monthEnd && counts.has(fuDay)) {
                          counts.get(fuDay).followups.push(l);
                        }
                      }

                      const isSameMonth = (d) => d.getMonth() === crmCalendarMonth.getMonth();

                      return days.map((d) => {
                        const iso = toISODateLocal(d);
                        const v = counts.get(iso) || { newLeads: [], followups: [] };
                        const newCount = v.newLeads.length;
                        const fuCount = v.followups.length;

                        return (
                          <button type="button"
                            key={iso}
                            onClick={() => {
                        setCrmDateFilter(iso);
                        setCrmDateFilterKind("all");
                        setShowCrmCalendar(false);
                      }}
                            style={{
                              textAlign: "left",
                              borderRadius: 14,
                              border: "1px solid #e2e8f0",
                              padding: 10,
                              background: isSameMonth(d) ? "#ffffff" : "#f8fafc",
                              minHeight: 86,
                              cursor: "pointer",
                            }}
                          >
                            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                              <div style={{ fontWeight: 800, color: isSameMonth(d) ? "#0f172a" : "#64748b" }}>{d.getDate()}</div>
                              <div style={{ display: "flex", gap: 6 }}>
                                {newCount > 0 && (
                              <button
                                type="button"
                                style={{ ...badge("blue"), cursor: "pointer", background: "#dbeafe" }}
                                onClick={(e) => {
                                  e.stopPropagation();
                                  setCrmDateFilter(iso);
                                  setCrmDateFilterKind("new");
                                  setShowCrmCalendar(false);
                                }}
                                title="Open NEW leads for this day"
                              >
                                New {newCount}
                              </button>
                            )}
                            {fuCount > 0 && (
                              <button
                                type="button"
                                style={{ ...badge("amber"), cursor: "pointer", background: "#fef3c7" }}
                                onClick={(e) => {
                                  e.stopPropagation();
                                  setCrmDateFilter(iso);
                                  setCrmDateFilterKind("fu");
                                  setShowCrmCalendar(false);
                                }}
                                title="Open FOLLOW-UPS for this day"
                              >
                                FU {fuCount}
                              </button>
                            )}
                              </div>
                            </div>
                            <div style={{ marginTop: 8, fontSize: 12, color: "#64748b" }}>
                              {newCount === 0 && fuCount === 0 ? "—" : "Tap to view"}
                            </div>
                          </button>
                        );
                      });
                    })()}
                  </div>
                </div>
              ) : leads.length === 0 ? (
                <p style={{ color: "#64748b" }}>No leads yet. Create leads from Call Queue or Last 7 Days.</p>
              ) : (
                <div style={{ overflowX: "auto" }}>
                  <table style={{ ...table, minWidth: 900 }}>
                    <thead>
                      <tr>
                        <th style={th}>Status</th>
                        <th style={th}>Property</th>
                        <th style={th}>Unit</th>
                        <th style={th}>Updated</th>
                        <th style={th}>Open</th>
                      </tr>
                    </thead>
                    <tbody>
                      {((() => {
                        const day = crmDateFilter;
                        const kind = crmDateFilterKind;
                        if (!day) return leads;
                        if (kind === "new") return (leads || []).filter((x) => (x.created_at || "").slice(0, 10) === day);
                        if (kind === "fu") return (leads || []).filter((x) => (x.follow_up_at || "").slice(0, 10) === day);
                        return (leads || []).filter((x) => {
                          const c = (x.created_at || "").slice(0, 10);
                          const f = (x.follow_up_at || "").slice(0, 10);
                          return c === day || f === day;
                        });
                      })())
                      .map((l) => (
                        <tr key={l.id}>
                          <td style={td}>
                            <span style={badge(l.status === "new" ? "blue" : l.status === "called" ? "green" : l.status === "follow_up" ? "amber" : "gray")}>
                              {l.status}
                            </span>
                          </td>
                          <td style={td}>
                            <div style={{ fontWeight: "bold" }}>{l.property_name}</div>
                            <div style={{ color: "#64748b", fontSize: 12 }}>{l.address_line}</div>
                          </td>
                          <td style={td}><b>{l.unit_number || ""}</b></td>
                          <td style={td}>{l.updated_at ? new Date(l.updated_at).toLocaleString() : ""}</td>
                          <td style={td}>
                            <button type="button" style={ui.smallBtn} onClick={() => {
                                  setSelectedLead(l);
                                  // init follow-up UI from lead
                                  if (l.follow_up_at) {
                                    const fu = new Date(l.follow_up_at);
                                    const local = new Date(fu.getTime() - fu.getTimezoneOffset() * 60000).toISOString().slice(0, 16);
                                    setLeadFollowUpAt(local);
                                  } else {
                                    setLeadFollowUpAt("");
                                  }
                                  const methods = (l.follow_up_methods || "").split(",").map((s) => s.trim().toLowerCase()).filter(Boolean);
                                  setLeadFollowUpMethods({
                                    email: methods.includes("email"),
                                    call: methods.includes("call") || methods.length === 0,
                                    text: methods.includes("text"),
                                    visit: methods.includes("visit"),
                                  });
                                }}>Open</button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>

            <div style={card}>
              <h3 style={{ marginTop: 0 }}>Lead Detail</h3>
              {!selectedLead ? (
                <p style={{ color: "#64748b" }}>Select a lead.</p>
              ) : (
                <>
                  <div style={{ fontWeight: "bold" }}>{selectedLead.property_name}</div>
                  <div style={{ color: "#64748b", fontSize: 12 }}>{selectedLead.address_line}</div>
                  <div style={{ marginTop: 8 }}>
                    Unit: <b>{selectedLead.unit_number || ""}</b>
                  </div>

                  <div style={{ marginTop: 12 }}>
                    <label>Status</label>
                    <select
                      value={selectedLead.status || "new"}
                      onChange={async (e) => {
                        let next = e.target.value;

                        // Legacy compatibility
                        if (next === "followup") next = "follow_up";
                        if (next === "closed_won") next = "closed";
                        if (next === "closed_lost") next = "lost";

                        await updateLead(selectedLead.id, { status: next });
                        setSelectedLead((prev) => (prev ? { ...prev, status: next } : prev));

                        // If switching into follow-up, prompt scheduler defaults
                        if (next === "follow_up") {
                          if (!leadFollowUpAt) {
                            const d = new Date();
                            d.setDate(d.getDate() + 1);
                            d.setHours(10, 0, 0, 0);
                            const local = new Date(d.getTime() - d.getTimezoneOffset() * 60000)
                              .toISOString()
                              .slice(0, 16);
                            setLeadFollowUpAt(local);
                            setLeadFollowUpMethods({ email: false, call: true, text: false, visit: false });
                          }
                          setTimeout(() => {
                            const el = document.getElementById("followup-datetime");
                            if (el) el.focus();
                          }, 50);
                        }
                      }}
                      style={{ ...ui.select, width: "100%" }}
                    >
                      <option value="new">New</option>
                      <option value="called">Called</option>
                      <option value="followup">Follow-up</option>
                      <option value="closed_won">Closed / Won</option>
                      <option value="closed_lost">Closed / Lost</option>
                    </select>
                  </div>
                  {selectedLead.status === "follow_up" ? (
                    <div style={{ marginTop: 12, borderTop: "1px solid #e2e8f0", paddingTop: 12 }}>
                      <div style={{ fontWeight: 800, marginBottom: 6 }}>Follow-up Scheduler</div>

                      <label style={{ display: "block", fontSize: 12, color: "#475569" }}>Follow-up date & time</label>
                      <input
                        id="followup-datetime"
                        type="datetime-local"
                        value={leadFollowUpAt}
                        onChange={(e) => setLeadFollowUpAt(e.target.value)}
                        style={{ ...ui.input, width: "100%", marginTop: 6 }}
                      />

                      <div style={{ marginTop: 10, display: "flex", gap: 12, flexWrap: "wrap" }}>
                        {["text", "call", "email", "visit"].map((m) => (
                          <label key={m} style={{ display: "flex", gap: 6, alignItems: "center", fontSize: 13, color: "#334155" }}>
                            <input
                              type="checkbox"
                              checked={!!leadFollowUpMethods[m]}
                              onChange={(e) => setLeadFollowUpMethods({ ...leadFollowUpMethods, [m]: e.target.checked })}
                            />
                            {m.toUpperCase()}
                          </label>
                        ))}
                      </div>

                      <div style={{ marginTop: 10, display: "flex", gap: 10, flexWrap: "wrap" }}>
                        <button type="button"
                          style={ui.primaryBtn}
                          onClick={async () => {
                            try {
                              const methods = Object.entries(leadFollowUpMethods)
                                .filter(([_, v]) => v)
                                .map(([k]) => k)
                                .join(",");

                              const followUpAtISO = leadFollowUpAt ? new Date(leadFollowUpAt).toISOString() : null;

                              await updateLead(selectedLead.id, {
                                status: "follow_up",
                                follow_up_at: followUpAtISO,
                                follow_up_methods: methods || null,
                              });

                              setStatus("✅ Follow-up saved.");
                            } catch (err) {
                              setStatus(`❌ Follow-up save error: ${err.message || "unknown error"}`);
                            }
                          }}
                        >
                          Save Follow-up
                        </button>

                        <button type="button"
                          style={ui.secondaryBtn}
                          onClick={async () => {
                            try {
                              setLeadFollowUpAt("");
                              setLeadFollowUpMethods({ email: false, call: true, text: false, visit: false });
                              await updateLead(selectedLead.id, { follow_up_at: null, follow_up_methods: null });
                              setStatus("✅ Follow-up cleared.");
                            } catch (err) {
                              setStatus(`❌ Follow-up clear error: ${err.message || "unknown error"}`);
                            }
                          }}
                        >
                          Clear
                        </button>
                      </div>
                    </div>
                  ) : null}


                  <div style={{ marginTop: 12 }}>
                    <label>Notes</label>
                    <textarea
                      value={selectedLead.notes || ""}
                      onChange={(e) => setSelectedLead((prev) => (prev ? { ...prev, notes: e.target.value } : prev))}
                      style={ui.textarea}
                      rows={6}
                      placeholder="Call notes..."
                    />
                    <button
                      style={{ ...ui.primaryBtn, width: "100%", marginTop: 10 }}
                      onClick={async () => {
                        await updateLead(selectedLead.id, { notes: selectedLead.notes || "" });
                        setStatus("✅ Notes saved.");
                      }}
                    >
                      Save Notes
                    </button>
                  </div>
                </>
              )}
            </div>
          </div>
        </>
      ) : null}

      {/* ---------------- Dashboard ---------------- */}
      {tab === "dashboard" ? (
        <>
          <div style={card}>
            <h2 style={{ marginTop: 0 }}>Dashboard</h2>

            <div style={{ display: "flex", gap: 12, flexWrap: "wrap", alignItems: "center" }}>
              <label>
                Date:&nbsp;
                <input type="date" value={dashDate} onChange={(e) => setDashDate(e.target.value)} style={ui.input} />
              </label>

              <label>
                City:&nbsp;
                <select value={dashCity} onChange={(e) => setDashCity(e.target.value)} style={ui.select}>
                  <option value="">All</option>
                  {cityOptions.map((c) => (
                    <option key={c} value={c}>{c}</option>
                  ))}
                </select>
              </label>

              <label>
                Zip:&nbsp;
                <select value={dashZip} onChange={(e) => setDashZip(e.target.value)} style={ui.select}>
                  <option value="">All</option>
                  {zipOptions.map((z) => (
                    <option key={z} value={z}>{z}</option>
                  ))}
                </select>
              </label>

              <label>
                Rep:&nbsp;
                <select value={dashRepFilter} onChange={(e) => setDashRepFilter(e.target.value)} style={ui.select}>
                  <option value="">All</option>
                  {reps.map((r) => (
                    <option key={r.id} value={r.id}>{r.name}</option>
                  ))}
                </select>
              </label>

              <button onClick={loadDashboard} style={ui.primaryBtn}>Refresh</button>
            </div>

            <div style={{ display: "flex", gap: 12, marginTop: 14, flexWrap: "wrap" }}>
              <div style={statBox}>
                <div style={statLabel}>Total Properties</div>
                <div style={statValue}>{dashTotals.totalProperties}</div>
              </div>
              <div style={statBox}>
                <div style={statLabel}>Total Reps</div>
                <div style={statValue}>{dashTotals.totalReps}</div>
              </div>
              <div style={statBox}>
                <div style={statLabel}>Units On Market (Selected Date)</div>
                <div style={statValue}>{dashTotals.totalUnitsOnMarket}</div>
              </div>
            </div>

            <div style={{ marginTop: 10, color: "#64748b" }}>
              {dashLoading ? "Loading..." : `Showing ${dashRows.reduce((s,g)=>s+(g.rows?.length||0),0)} properties (filtered).`}
            </div>
          </div>

          <div style={{ marginTop: 16 }}>
            {/* Last 7 Days Leads Chart */}
            <div style={card}>
              <div style={{ display: "flex", justifyContent: "space-between", gap: 12, flexWrap: "wrap", alignItems: "center" }}>
                <h3 style={{ marginTop: 0, marginBottom: 0 }}>
                  {dashChartMetric === "leads" ? "New CRM Leads Created (Last 7 Days)" : "New Units Appeared (Last 7 Days)"}
                </h3>
                <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
                  <span style={{ color: "#64748b", fontSize: 12 }}>Metric</span>
                  <select value={dashChartMetric} onChange={(e) => setDashChartMetric(e.target.value)} style={ui.select}>
                    <option value="appeared">Appeared Units</option>
                    <option value="leads">CRM Leads</option>
                  </select>
                </div>
              </div>
              {dashLeadSeries.length === 0 ? (
                <div style={{ color: "#64748b" }}>No data yet for this range.</div>
              ) : (
                <div style={{ display: "flex", gap: 10, alignItems: "flex-end", height: 140, paddingTop: 10 }}>
                  {dashLeadSeries.map((pt) => {
                    const max = Math.max(...dashLeadSeries.map((x) => x.count), 1);
                    const h = Math.round((pt.count / max) * 110);
                    return (
                      <div key={pt.date} style={{ width: 70, textAlign: "center" }}>
                        <div style={{ fontSize: 12, color: "#475569", marginBottom: 6 }}><b>{pt.count}</b></div>
                        <div
                          title={`${pt.date}: ${pt.count}`}
                          style={{
                            height: h,
                            borderRadius: 10,
                            border: "1px solid #1d4ed8",
                            background: "#93c5fd",
                          }}
                        />
                        <div style={{ fontSize: 11, color: "#64748b", marginTop: 6 }}>
                          {pt.date.slice(5)}
                        </div>
                      </div>
                    );
                  })}
                </div>
              )}
            </div>

            <div style={{ height: 14 }} />

            {dashLoading ? (
              <p>Loading...</p>
            ) : dashRows.length === 0 ? (
              <p style={{ color: "#64748b" }}>No properties found for these filters.</p>
            ) : (
              <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
                {dashRows.map((rg) => (
                  <div key={rg.rep_id || "__unassigned__"} style={card}>
                    <h3 style={{ margin: 0 }}>
                      {rg.rep_name} <span style={badge("gray")}>{rg.total_units} units</span>
                    </h3>

                    <div style={{ overflowX: "auto", marginTop: 10 }}>
                      <table style={{ ...table, minWidth: 900 }}>
                        <thead>
                          <tr>
                            <th style={th}>Property</th>
                            <th style={th}>Address</th>
                            <th style={th}>Units On Market</th>
                            <th style={th}>Snapshot Date</th>
                            <th style={th}>Open</th>
                          </tr>
                        </thead>
                        <tbody>
                          {rg.rows.map((r) => (
                            <tr key={r.property_id}>
                              <td style={td}>{r.property_name}</td>
                              <td style={td}>{r.address_line}</td>
                              <td style={td}><b>{r.units_on_market}</b></td>
                              <td style={td}>
                                {r.snapshot_date}
                                {r.snapshot_note === "fallback" ? (
                                  <span style={{ color: "#b45309" }}> (fallback)</span>
                                ) : r.snapshot_note === "none" ? (
                                  <span style={{ color: "#64748b" }}> (none)</span>
                                ) : (
                                  <span style={{ color: "#15803d" }}> (exact)</span>
                                )}
                              </td>
                              <td style={td}>
                                <button
                                  onClick={() => {
                                    setCurrentDate(dashDate);
                                    setCurrentPropertyId(r.property_id);
                                    setTab("currentUnits");
                                  }}
                                  style={ui.smallBtn}
                                >
                                  View Units
                                </button>
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </>
      ) : null}

      {/* ---------------- Current Units (On Market) ---------------- */}
      {tab === "currentUnits" ? (
        <>
          <div style={card}>
            <h2 style={{ marginTop: 0 }}>Current Units On Market</h2>

            <div style={{ display: "flex", gap: 12, flexWrap: "wrap", alignItems: "center" }}>
              <label>
                Date:&nbsp;
                <input type="date" value={currentDate} onChange={(e) => setCurrentDate(e.target.value)} style={ui.input} />
              </label>

              <label>
                City:&nbsp;
                <select value={currentCity} onChange={(e) => setCurrentCity(e.target.value)} style={ui.select}>
                  <option value="">All</option>
                  {cityOptions.map((c) => (
                    <option key={c} value={c}>{c}</option>
                  ))}
                </select>
              </label>

              <label>
                Zip:&nbsp;
                <select value={currentZip} onChange={(e) => setCurrentZip(e.target.value)} style={ui.select}>
                  <option value="">All</option>
                  {zipOptions.map((z) => (
                    <option key={z} value={z}>{z}</option>
                  ))}
                </select>
              </label>

              <label>
                Property:&nbsp;
                <select value={currentPropertyId} onChange={(e) => setCurrentPropertyId(e.target.value)} style={ui.select}>
                  <option value="">All</option>
                  {scopedProperties.map((p) => (
                    <option key={p.id} value={p.id}>{p.name}</option>
                  ))}
                </select>
              </label>

              <label>
                Sort:&nbsp;
                <select value={currentSortBy} onChange={(e) => setCurrentSortBy(e.target.value)} style={ui.select}>
                  <option value="property">Property</option>
                  <option value="rep">Rep</option>
                </select>
              </label>

              <button onClick={loadCurrentUnits} style={ui.primaryBtn}>Refresh</button>
            </div>

            <div style={{ marginTop: 10, color: "#475569" }}>
              Properties: <b>{currentGroups.length}</b> • Units: <b>{currentGroups.reduce((sum, g) => sum + (g.count || 0), 0)}</b>
              {currentLoading ? " (loading...)" : ""}
            </div>

            <div style={{ marginTop: 8, color: "#64748b" }}>
              If a property has no snapshot exactly on the selected day, the app uses the most recent snapshot <b>before</b> that date and marks it “fallback”.
            </div>
          </div>

          <div style={{ marginTop: 16 }}>
            {currentLoading ? (
              <p>Loading...</p>
            ) : currentGroups.length === 0 ? (
              <p style={{ color: "#64748b" }}>No snapshots found for that date (or earlier) for these filters.</p>
            ) : (
              <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
                {currentGroups.map((g) => (
                  <div key={g.property_id} style={card}>
                    <div style={{ display: "flex", justifyContent: "space-between", gap: 12, flexWrap: "wrap" }}>
                      <div>
                        <h3 style={{ margin: 0 }}>
                          {g.property_name} <span style={badge("gray")}>{g.count} units</span>
                        </h3>
                        <div style={{ marginTop: 4, color: "#64748b" }}>{g.address_line || "—"}</div>
                        <div style={{ marginTop: 4, color: "#64748b" }}>
                          Rep: <b>{g.rep_name}</b>
                        </div>
                      </div>

                      <div style={{ color: "#64748b" }}>
                        Snapshot: <b>{g.snapshot_date}</b>{" "}
                        {g.from_date === "fallback" ? <span style={{ color: "#b45309" }}> (fallback)</span> : <span style={{ color: "#15803d" }}> (exact)</span>}
                      </div>
                    </div>

                    <div style={{ overflowX: "auto", marginTop: 10 }}>
                      <table style={{ ...table, minWidth: 520 }}>
                        <thead>
                          <tr>
                            <th style={th}>Unit #</th>
                            <th style={th}>Unit Key</th>
                            <th style={th}>Lead</th>
                          </tr>
                        </thead>
                        <tbody>
                          {g.rows.map((r, idx) => (
                            <tr key={`${g.property_id}-${r.unit_key}-${idx}`}>
                              <td style={td}><b>{r.unit_number ?? ""}</b></td>
                              <td style={td}>{r.unit_key}</td>
                              <td style={td}>
                                <button type="button"
                                  style={ui.smallBtn}
                                  onClick={() =>
                                    upsertLeadFromUnit({
                                      property_id: g.property_id,
                                      property_name: g.property_name,
                                      address_line: g.address_line,
                                      unit_number: r.unit_number,
                                      unit_key: r.unit_key,
                                    })
                                  }
                                >
                                  Create Lead
                                </button>
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </>
      ) : null}

      {/* ---------------- Daily Units (Changes) ---------------- */}
      {tab === "dailyUnits" ? (
        <>
          <div style={card}>
            <h2 style={{ marginTop: 0 }}>Daily Units — Changes Only</h2>

            <div style={{ display: "flex", gap: 12, flexWrap: "wrap", alignItems: "center" }}>
              <label>
                Date:&nbsp;
                <input type="date" value={dailyDate} onChange={(e) => setDailyDate(e.target.value)} style={ui.input} />
              </label>

              <label>
                Source:&nbsp;
                <select value={dailySource} onChange={(e) => setDailySource(e.target.value)} style={ui.select}>
                  <option value="all">All</option>
                  <option value="snapshot">snapshot</option>
                  <option value="rentcafe">rentcafe</option>
                  <option value="scan">scan</option>
                  <option value="sightmap">sightmap</option>
                </select>
              </label>

              <label>
                Type:&nbsp;
                <select value={dailyEventType} onChange={(e) => setDailyEventType(e.target.value)} style={ui.select}>
                  <option value="appeared">appeared (new)</option>
                  <option value="disappeared">disappeared (off market)</option>
                </select>
              </label>

              <button onClick={loadDailyUnits} style={ui.primaryBtn}>Refresh</button>
            </div>

            <div style={{ marginTop: 10, color: "#475569" }}>
              Showing <b>{dailyRows.length}</b> rows {dailyLoading ? "(loading...)" : ""}
            </div>
          </div>

          <div style={{ marginTop: 16 }}>
            {dailyLoading ? (
              <p>Loading...</p>
            ) : dailyRows.length === 0 ? (
              <p style={{ color: "#64748b" }}>No rows for this date/filter. (This view shows changes only.)</p>
            ) : (
              <div style={{ overflowX: "auto" }}>
                <table style={table}>
                  <thead>
                    <tr>
                      <th style={th}>Date</th>
                      <th style={th}>Property</th>
                      <th style={th}>Unit #</th>
                      <th style={th}>Unit Key</th>
                      <th style={th}>Type</th>
                      <th style={th}>Source</th>
                      <th style={th}>Lead</th>
                    </tr>
                  </thead>

                  <tbody>
                    {dailyRows.map((r, idx) => (
                      <tr key={`${r.event_date}-${r.property_id}-${r.unit_key}-${idx}`}>
                        <td style={td}>{r.event_date}</td>
                        <td style={td}>{r.property_name}</td>
                        <td style={td}><b>{r.unit_number ?? ""}</b></td>
                        <td style={td}>{r.unit_key}</td>
                        <td style={td}>{r.event_type}</td>
                        <td style={td}>{r.source}</td>
                        <td style={td}>
                          <button type="button"
                            style={ui.smallBtn}
                            onClick={() =>
                              upsertLeadFromUnit({
                                property_id: r.property_id,
                                property_name: r.property_name,
                                address_line: formatAddressLine(properties.find((p) => p.id === r.property_id) || {}),
                                unit_number: r.unit_number,
                                unit_key: r.unit_key,
                              })
                            }
                          >
                            Create Lead
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>

                <p style={{ marginTop: 10, color: "#64748b" }}>
                  Tip: use <b>Last 7 Days</b> for the weekly workflow.
                </p>
              </div>
            )}
          </div>
        </>
      ) : null}

      {/* ---------------- Properties ---------------- */}
      {tab === "properties" ? (
        <>
          <div style={card}>
            <h2 style={{ marginTop: 0 }}>Add Property</h2>
            <form onSubmit={addProperty}>
              <div style={{ marginBottom: 12 }}>
                <label>Property Name</label>
                <br />
                <input value={pName} onChange={(e) => setPName(e.target.value)} style={ui.longInput} required />
              </div>

              <div style={{ marginBottom: 12 }}>
                <label>Website URL</label>
                <br />
                <input value={pUrl} onChange={(e) => setPUrl(e.target.value)} style={ui.longInput} placeholder="https://..." required />
              </div>

              <div style={{ marginBottom: 12 }}>
                <label>Platform</label>
                <br />
                <select value={pPlatform} onChange={(e) => setPPlatform(e.target.value)} style={{ ...ui.select, width: 260 }}>
                  <option value="">Select platform</option>
                  {PLATFORM_OPTIONS.map((o) => (
                    <option key={o.value} value={o.value}>{o.label}</option>
                  ))}
                </select>
              </div>

              <div style={{ marginBottom: 12 }}>
                <label>Address</label>
                <br />
                <input value={pAddress} onChange={(e) => setPAddress(e.target.value)} style={ui.longInput} placeholder="123 Main St" />
              </div>

              <div style={{ marginBottom: 12, display: "flex", gap: 10, flexWrap: "wrap" }}>
                <div>
                  <label>City</label>
                  <br />
                  <input value={pCity} onChange={(e) => setPCity(e.target.value)} style={ui.input} placeholder="Los Angeles" />
                </div>
                <div>
                  <label>Zip</label>
                  <br />
                  <input value={pZip} onChange={(e) => setPZip(e.target.value)} style={ui.input} placeholder="90001" />
                </div>
              </div>

              <button type="submit" style={ui.primaryBtn}>Save Property</button>
            </form>
          </div>

          <div style={{ marginTop: 16 }}>
            <h2>Properties ({properties.length})</h2>

            {loadingProps ? (
              <p>Loading...</p>
            ) : properties.length === 0 ? (
              <p>No properties yet.</p>
            ) : (
              <div style={{ overflowX: "auto" }}>
                <table style={table}>
                  <thead>
                    <tr>
                      <th style={th}>Name</th>
                      <th style={th}>Address</th>
                      <th style={th}>City</th>
                      <th style={th}>Zip</th>
                      <th style={th}>URL</th>
                      <th style={th}>Platform</th>
                      <th style={th}>Assigned Rep</th>
                      <th style={th}>Created</th>
                      <th style={th}>Actions</th>
                    </tr>
                  </thead>

                  <tbody>
                    {properties.map((p) => (
                      <tr key={p.id}>
                        <td style={td}>
                          {editingPropId === p.id ? (
                            <input value={editName} onChange={(e) => setEditName(e.target.value)} style={{ width: 220, padding: 6 }} />
                          ) : (
                            p.name
                          )}
                        </td>

                        <td style={td}>
                          {editingPropId === p.id ? (
                            <input value={editAddress} onChange={(e) => setEditAddress(e.target.value)} style={{ width: 220, padding: 6 }} />
                          ) : (
                            p.address ?? ""
                          )}
                        </td>

                        <td style={td}>
                          {editingPropId === p.id ? (
                            <input value={editCity} onChange={(e) => setEditCity(e.target.value)} style={{ width: 160, padding: 6 }} />
                          ) : (
                            p.city ?? ""
                          )}
                        </td>

                        <td style={td}>
                          {editingPropId === p.id ? (
                            <input value={editZip} onChange={(e) => setEditZip(e.target.value)} style={{ width: 110, padding: 6 }} />
                          ) : (
                            p.zip ?? ""
                          )}
                        </td>

                        <td style={td}>
                          {editingPropId === p.id ? (
                            <input value={editUrl} onChange={(e) => setEditUrl(e.target.value)} style={{ width: 320, padding: 6 }} />
                          ) : (
                            <a href={p.url} target="_blank" rel="noreferrer">{p.url}</a>
                          )}
                        </td>

                        <td style={td}>
                          {editingPropId === p.id ? (
                            <select value={editPlatform} onChange={(e) => setEditPlatform(e.target.value)} style={{ padding: 6, width: 160 }}>
                              {!platformValuesSet.has(normalizePlatformValue(editPlatform)) && (
                                <option value={editPlatform}>{platformLabel(editPlatform)}</option>
                              )}
                              {PLATFORM_OPTIONS.map((o) => (
                                <option key={o.value} value={o.value}>{o.label}</option>
                              ))}
                            </select>
                          ) : (
                            platformLabel(p.platform)
                          )}
                        </td>

                        <td style={td}>
                          <select value={p.rep_id ?? ""} onChange={(e) => assignRep(p.id, e.target.value)} style={{ padding: 6, width: 220 }}>
                            <option value="">Unassigned</option>
                            {reps.map((r) => (
                              <option key={r.id} value={r.id}>{r.name}</option>
                            ))}
                          </select>
                        </td>

                        <td style={td}>{p.created_at ? new Date(p.created_at).toLocaleString() : ""}</td>

                        <td style={td}>
                          {editingPropId === p.id ? (
                            <>
                              <button onClick={saveEditProperty} style={{ marginRight: 8 }}>Save</button>
                              <button onClick={cancelEditProperty} style={{ marginRight: 8 }}>Cancel</button>
                            </>
                          ) : (
                            <button onClick={() => startEditProperty(p)} style={{ marginRight: 8 }}>Edit</button>
                          )}
                          <button onClick={() => deleteProperty(p.id)}>Delete</button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>

                <p style={{ marginTop: 10, color: "#64748b" }}>
                  Tip: address/city/zip power filters across Dashboard + Current Units.
                </p>
              </div>
            )}
          </div>
        </>
      ) : null}

      {/* ---------------- Reps ---------------- */}
      {tab === "reps" ? (
        <>
          <div style={card}>
            <h2 style={{ marginTop: 0 }}>Add Rep</h2>
            <form onSubmit={addRep}>
              <div style={{ marginBottom: 12 }}>
                <label>Rep Name</label>
                <br />
                <input value={repName} onChange={(e) => setRepName(e.target.value)} style={{ ...ui.input, width: 420 }} placeholder="e.g., Jairo" required />
              </div>
              <button type="submit" style={ui.primaryBtn}>Save Rep</button>
            </form>
          </div>

          <div style={{ marginTop: 16 }}>
            <h2>Reps ({reps.length})</h2>

            {loadingReps ? (
              <p>Loading...</p>
            ) : reps.length === 0 ? (
              <p>No reps yet.</p>
            ) : (
              <div style={{ overflowX: "auto" }}>
                <table style={table}>
                  <thead>
                    <tr>
                      <th style={th}>Name</th>
                      <th style={th}>Created</th>
                      <th style={th}>Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {reps.map((r) => (
                      <tr key={r.id}>
                        <td style={td}>
                          {editingRepId === r.id ? (
                            <input value={editingRepName} onChange={(e) => setEditingRepName(e.target.value)} style={{ width: 320, padding: 6 }} />
                          ) : (
                            <b>{r.name}</b>
                          )}
                        </td>
                        <td style={td}>{r.created_at ? new Date(r.created_at).toLocaleString() : ""}</td>
                        <td style={td}>
                          {editingRepId === r.id ? (
                            <>
                              <button onClick={saveEditRep} style={{ marginRight: 8 }}>Save</button>
                              <button onClick={cancelEditRep}>Cancel</button>
                            </>
                          ) : (
                            <>
                              <button onClick={() => startEditRep(r)} style={{ marginRight: 8 }}>Edit</button>
                              <button onClick={() => deleteRep(r.id)}>Delete</button>
                            </>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>

                <p style={{ marginTop: 10, color: "#64748b" }}>
                  Phase 1: Admin uses “View as Rep” to validate rep-scoped UI.
                </p>
              </div>
            )}
          </div>
        </>
      ) : null}
    </div>
  );
}

// ---------------------------
// Styles / Theme
// ---------------------------
const ui = {
  bg: "#F6F7FB",
  header: {
    background: "#0f172a",
    color: "white",
    borderRadius: 14,
    padding: "14px 16px",
    display: "flex",
    justifyContent: "space-between",
    alignItems: "center",
    gap: 12,
    marginBottom: 12,
  },
  tabs: {
    display: "flex",
    gap: 8,
    marginBottom: 16,
    flexWrap: "wrap",
    background: "white",
    padding: 10,
    borderRadius: 14,
    border: "1px solid #e5e7eb",
  },
  pill: {
    background: "#111827",
    border: "1px solid #334155",
    color: "white",
    borderRadius: 14,
    padding: "8px 10px",
    display: "flex",
    flexDirection: "column",
    gap: 2,
  },
  selectDark: {
    background: "#0b1220",
    color: "white",
    border: "1px solid #334155",
    borderRadius: 10,
    padding: "6px 10px",
    marginTop: 4,
    minWidth: 220,
  },
  input: {
    padding: 8,
    borderRadius: 10,
    border: "1px solid #cbd5e1",
    background: "white",
    minWidth: 180,
  },
  select: {
    padding: 8,
    borderRadius: 10,
    border: "1px solid #cbd5e1",
    background: "white",
    minWidth: 200,
  },
  longInput: {
    width: 520,
    padding: 8,
    borderRadius: 10,
    border: "1px solid #cbd5e1",
    background: "white",
  },
  textarea: {
    width: "100%",
    padding: 10,
    borderRadius: 12,
    border: "1px solid #cbd5e1",
    background: "white",
    marginTop: 6,
  },
  secondaryBtn: {
      background: "#ffffff",
      border: "1px solid #cbd5e1",
      color: "#0f172a",
      borderRadius: 12,
      padding: "10px 12px",
      cursor: "pointer",
      fontWeight: 700,
    },
    primaryBtn: {
    padding: "10px 14px",
    borderRadius: 12,
    border: "1px solid #1d4ed8",
    background: "#2563eb",
    color: "white",
    cursor: "pointer",
    fontWeight: "bold",
  },
  logoutBtn: {
    padding: "10px 14px",
    borderRadius: 12,
    border: "1px solid #ef4444",
    background: "#ef4444",
    color: "white",
    cursor: "pointer",
    fontWeight: "bold",
  },

    smallBtn: {
    padding: "8px 10px",
    borderRadius: 10,
    border: "1px solid #cbd5e1",
    background: "white",
    cursor: "pointer",
  },
  innerCard: {
    border: "1px solid #e5e7eb",
    borderRadius: 14,
    padding: 12,
    background: "#f9fafb",
  },
};

const tabBtn = (active) => ({
  padding: "10px 12px",
  borderRadius: 12,
  border: "1px solid " + (active ? "#1d4ed8" : "#cbd5e1"),
  background: active ? "#2563eb" : "#fff",
  color: active ? "#fff" : "#0f172a",
  cursor: "pointer",
  fontWeight: active ? "bold" : "normal",
});

const badge = (kind) => {
  const map = {
    blue: { bg: "#DBEAFE", fg: "#1D4ED8", br: "#BFDBFE" },
    green: { bg: "#DCFCE7", fg: "#166534", br: "#BBF7D0" },
    amber: { bg: "#FEF3C7", fg: "#92400E", br: "#FDE68A" },
    gray: { bg: "#F1F5F9", fg: "#334155", br: "#E2E8F0" },
    modalOverlay: {
      position: "fixed",
      top: 0, left: 0, right: 0, bottom: 0,
      background: "rgba(15, 23, 42, 0.55)",
      display: "flex",
      alignItems: "center",
      justifyContent: "center",
      padding: 16,
      zIndex: 50,
    },
    modalCard: {
      width: "min(980px, 96vw)",
      maxHeight: "86vh",
      overflow: "auto",
      background: "#fff",
      borderRadius: 18,
      border: "1px solid #e2e8f0",
      padding: 16,
      boxShadow: "0 10px 30px rgba(15,23,42,0.18)",
    },
    listItemBtn: {
      textAlign: "left",
      borderRadius: 14,
      border: "1px solid #e2e8f0",
      padding: 10,
      background: "#fff",
      cursor: "pointer",
    },

  };
  const c = map[kind] || map.gray;
  return {
    display: "inline-block",
    padding: "4px 10px",
    borderRadius: 999,
    fontSize: 12,
    fontWeight: "bold",
    background: c.bg,
    color: c.fg,
    border: `1px solid ${c.br}`,
    marginLeft: 8,
  };
};

const card = {
  padding: 16,
  border: "1px solid #e5e7eb",
  borderRadius: 14,
  background: "white",
};

const table = {
  borderCollapse: "collapse",
  width: "100%",
  minWidth: 900,
  border: "1px solid #e5e7eb",
  background: "white",
};

const th = {
  textAlign: "left",
  padding: 10,
  borderBottom: "1px solid #e5e7eb",
  background: "#f1f5f9",
  color: "#0f172a",
};

const td = {
  padding: 10,
  borderBottom: "1px solid #f1f5f9",
  verticalAlign: "top",
  color: "#0f172a",
};

const statBox = {
  border: "1px solid #e5e7eb",
  borderRadius: 14,
  padding: 12,
  minWidth: 200,
  background: "#f9fafb",
};

const statLabel = { color: "#64748b", fontSize: 13 };
const statValue = { fontSize: 24, fontWeight: "bold", color: "#0f172a" };

