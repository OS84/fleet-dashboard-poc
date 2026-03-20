const express = require("express");
const multer = require("multer");
const path = require("path");
const db = require("./db");

const app = express();
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 50 * 1024 * 1024 } });

app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

// --- API Routes ---

// Upload CSVs for a given date
app.post("/api/upload", upload.fields([
  { name: "customers", maxCount: 1 },
  { name: "vehicles", maxCount: 1 },
]), (req, res) => {
  try {
    const custFile = req.files?.customers?.[0];
    const vehFile = req.files?.vehicles?.[0];
    if (!custFile || !vehFile) {
      return res.status(400).json({ error: "Both customers and vehicles CSV files are required." });
    }

    const uploadDate = req.body.upload_date || new Date().toISOString().slice(0, 10);
    const baseRate = parseFloat(req.body.base_rate) || parseFloat(db.getSetting("base_rate_annual", "8200"));

    const custText = custFile.buffer.toString("utf-8");
    const vehText = vehFile.buffer.toString("utf-8");

    // Return CSV texts to client for processing, or process server-side
    // We send back the raw text so the client-side buildDB logic stays consistent.
    // The client processes, computes KPIs, then POST /api/save-snapshot with the results.
    res.json({
      ok: true,
      upload_date: uploadDate,
      base_rate: baseRate,
      customers_csv: custText,
      vehicles_csv: vehText,
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Save processed snapshot (client sends built DB + KPIs)
app.post("/api/snapshots", (req, res) => {
  try {
    const { upload_date, base_rate, customers, vehicles, kpis } = req.body;
    if (!upload_date || !customers || !vehicles || !kpis) {
      return res.status(400).json({ error: "Missing required fields: upload_date, customers, vehicles, kpis" });
    }

    const snapId = db.saveSnapshot(upload_date, base_rate || 8200, customers, vehicles, kpis);
    db.setSetting("base_rate_annual", String(base_rate || 8200));

    res.json({ ok: true, snapshot_id: snapId, upload_date });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// List all snapshots
app.get("/api/snapshots", (req, res) => {
  res.json(db.getSnapshots());
});

// Get latest snapshot data
app.get("/api/snapshots/latest", (req, res) => {
  const snap = db.getLatestSnapshot();
  if (!snap) return res.json({ snapshot: null, customers: [], vehicles: [] });

  const customers = db.getCustomers(snap.id);
  const vehicles = db.getVehicles(snap.id);
  res.json({ snapshot: snap, customers, vehicles });
});

// Get specific snapshot data
app.get("/api/snapshots/:id", (req, res) => {
  const snapId = parseInt(req.params.id);
  const snaps = db.getSnapshots();
  const snap = snaps.find(s => s.id === snapId);
  if (!snap) return res.status(404).json({ error: "Snapshot not found" });

  const customers = db.getCustomers(snapId);
  const vehicles = db.getVehicles(snapId);
  res.json({ snapshot: snap, customers, vehicles });
});

// Delete a snapshot
app.delete("/api/snapshots/:id", (req, res) => {
  const snapId = parseInt(req.params.id);
  const d = db.getDb();
  d.prepare("DELETE FROM daily_kpis WHERE snapshot_id = ?").run(snapId);
  d.prepare("DELETE FROM customers WHERE snapshot_id = ?").run(snapId);
  d.prepare("DELETE FROM vehicles WHERE snapshot_id = ?").run(snapId);
  d.prepare("DELETE FROM snapshots WHERE id = ?").run(snapId);
  res.json({ ok: true });
});

// Get daily KPIs for trends
app.get("/api/daily-kpis", (req, res) => {
  const { start, end } = req.query;
  if (start && end) {
    res.json(db.getDailyKpis(start, end));
  } else {
    res.json(db.getAllDailyKpis());
  }
});

// Settings
app.get("/api/settings/:key", (req, res) => {
  const val = db.getSetting(req.params.key, null);
  res.json({ key: req.params.key, value: val });
});

app.put("/api/settings/:key", (req, res) => {
  const { value } = req.body;
  if (value == null) return res.status(400).json({ error: "value is required" });
  db.setSetting(req.params.key, value);
  res.json({ ok: true, key: req.params.key, value });
});

// Serve dashboard
app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, "0.0.0.0", () => {
  console.log(`Fleet Dashboard running at http://localhost:${PORT}`);
  console.log(`Share with your team: http://<your-server-ip>:${PORT}`);
});
