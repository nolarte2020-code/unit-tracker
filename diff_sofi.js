import fs from "fs";
import path from "path";

function loadJson(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function main() {
  const dir = "snapshots";
  if (!fs.existsSync(dir)) {
    console.log("No snapshots folder found. Run snapshot_sofi.js first.");
    process.exit(1);
  }

  const files = fs
    .readdirSync(dir)
    .filter((f) => f.startsWith("sofi_") && f.endsWith(".json"))
    .sort(); // timestamped names sort correctly

  if (files.length < 2) {
    console.log("Need at least 2 snapshots to compare. Run snapshot twice.");
    process.exit(0);
  }

  const prevFile = path.join(dir, files[files.length - 2]);
  const currFile = path.join(dir, files[files.length - 1]);

  const prev = loadJson(prevFile);
  const curr = loadJson(currFile);

  const prevKeys = new Set(prev.map((u) => `${u.unit_id}`)); // unit_id is safest
  const added = curr.filter((u) => !prevKeys.has(`${u.unit_id}`));

  console.log(`Previous: ${prevFile}`);
  console.log(`Current:  ${currFile}`);
  console.log("");

  if (added.length === 0) {
    console.log("No NEW units since last snapshot.");
    process.exit(0);
  }

  console.log("NEW units since last snapshot:");
  console.log(JSON.stringify(added, null, 2));
}

main();
