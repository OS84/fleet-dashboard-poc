const Database = require("better-sqlite3");
const path = require("path");

const DB_PATH = path.join(__dirname, "fleet.db");

let db;

function getDb() {
  if (!db) {
    db = new Database(DB_PATH);
    db.pragma("journal_mode = WAL");
    db.pragma("foreign_keys = ON");
    initSchema();
  }
  return db;
}

function initSchema() {
  db.exec(`
    -- Each daily upload is a snapshot
    CREATE TABLE IF NOT EXISTS snapshots (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      upload_date TEXT NOT NULL,
      uploaded_at TEXT NOT NULL DEFAULT (datetime('now')),
      customers_count INTEGER NOT NULL DEFAULT 0,
      vehicles_count INTEGER NOT NULL DEFAULT 0,
      base_rate_annual REAL NOT NULL DEFAULT 8200,
      UNIQUE(upload_date)
    );

    -- Customers per snapshot
    CREATE TABLE IF NOT EXISTS customers (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      snapshot_id INTEGER NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE,
      customer_id TEXT NOT NULL,
      name TEXT,
      dl_number TEXT,
      tlc_number TEXT,
      insurance_annual_rate REAL,
      insurance_daily_rate REAL,
      rate_tier TEXT,
      has_active_deal INTEGER NOT NULL DEFAULT 0,
      deal_number TEXT,
      vin TEXT,
      is_lead INTEGER NOT NULL DEFAULT 1,
      lead_stage TEXT,
      created_at TEXT,
      cust_deal_date TEXT,
      cust_deal_end TEXT
    );

    -- Vehicles per snapshot
    CREATE TABLE IF NOT EXISTS vehicles (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      snapshot_id INTEGER NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE,
      vehicle_id TEXT NOT NULL,
      type_make_model TEXT,
      vin TEXT,
      plate TEXT,
      diamond TEXT,
      diamond_expiration_date TEXT,
      has_active_deal INTEGER NOT NULL DEFAULT 0,
      deal_number TEXT,
      deal_start_date TEXT,
      deal_end_date TEXT,
      sales_channel TEXT,
      vehicle_location TEXT,
      insurance_annual_rate REAL,
      insurance_daily_rate REAL,
      insurance_rate_source TEXT,
      current_driver_customer_id TEXT,
      last_driver_customer_id TEXT,
      last_driver_annual_rate REAL,
      status_changed_at TEXT,
      color TEXT,
      plate_status TEXT,
      plate_expiration TEXT,
      deal_status_raw TEXT,
      tlc_number TEXT,
      short_vin TEXT
    );

    -- Aggregated daily KPIs (computed on upload for fast querying)
    CREATE TABLE IF NOT EXISTS daily_kpis (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      snapshot_id INTEGER NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE,
      upload_date TEXT NOT NULL,
      total_vehicles INTEGER,
      active_vehicles INTEGER,
      vacant_vehicles INTEGER,
      total_customers INTEGER,
      active_customers INTEGER,
      leads_count INTEGER,
      daily_spend REAL,
      money_on_floor REAL,
      efficiency_score INTEGER,
      tier_low INTEGER,
      tier_med INTEGER,
      tier_high INTEGER,
      lead_tier_low INTEGER,
      lead_tier_med INTEGER,
      lead_tier_high INTEGER,
      UNIQUE(upload_date)
    );

    -- Settings (base rate, etc.)
    CREATE TABLE IF NOT EXISTS settings (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_customers_snapshot ON customers(snapshot_id);
    CREATE INDEX IF NOT EXISTS idx_vehicles_snapshot ON vehicles(snapshot_id);
    CREATE INDEX IF NOT EXISTS idx_snapshots_date ON snapshots(upload_date);
    CREATE INDEX IF NOT EXISTS idx_daily_kpis_date ON daily_kpis(upload_date);
  `);
}

// Save a processed snapshot
function saveSnapshot(uploadDate, baseRate, customers, vehicles, kpis) {
  const d = getDb();
  const txn = d.transaction(() => {
    // Upsert snapshot (replace if same date)
    const existing = d.prepare("SELECT id FROM snapshots WHERE upload_date = ?").get(uploadDate);
    if (existing) {
      // Delete old data for this date
      d.prepare("DELETE FROM customers WHERE snapshot_id = ?").run(existing.id);
      d.prepare("DELETE FROM vehicles WHERE snapshot_id = ?").run(existing.id);
      d.prepare("DELETE FROM daily_kpis WHERE snapshot_id = ?").run(existing.id);
      d.prepare("DELETE FROM snapshots WHERE id = ?").run(existing.id);
    }

    const snap = d.prepare(
      "INSERT INTO snapshots (upload_date, customers_count, vehicles_count, base_rate_annual) VALUES (?, ?, ?, ?)"
    ).run(uploadDate, customers.length, vehicles.length, baseRate);
    const snapId = snap.lastInsertRowid;

    // Insert customers
    const custStmt = d.prepare(`INSERT INTO customers
      (snapshot_id, customer_id, name, dl_number, tlc_number, insurance_annual_rate, insurance_daily_rate,
       rate_tier, has_active_deal, deal_number, vin, is_lead, lead_stage, created_at, cust_deal_date, cust_deal_end)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);
    for (const c of customers) {
      custStmt.run(snapId, c.id, c.name, c.dl_number, c.tlc_number,
        c.insurance_annual_rate, c.insurance_daily_rate, c.rate_tier,
        c.has_active_deal ? 1 : 0, c.deal_number, c.vin,
        c.is_lead ? 1 : 0, c.lead_stage, c.created_at, c.cust_deal_date, c.cust_deal_end);
    }

    // Insert vehicles
    const vehStmt = d.prepare(`INSERT INTO vehicles
      (snapshot_id, vehicle_id, type_make_model, vin, plate, diamond, diamond_expiration_date,
       has_active_deal, deal_number, deal_start_date, deal_end_date, sales_channel, vehicle_location,
       insurance_annual_rate, insurance_daily_rate, insurance_rate_source,
       current_driver_customer_id, last_driver_customer_id, last_driver_annual_rate,
       status_changed_at, color, plate_status, plate_expiration, deal_status_raw, tlc_number, short_vin)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);
    for (const v of vehicles) {
      vehStmt.run(snapId, v.id, v.type_make_model, v.vin, v.plate, v.diamond, v.diamond_expiration_date,
        v.has_active_deal ? 1 : 0, v.deal_number, v.deal_start_date, v.deal_end_date,
        v.sales_channel, v.vehicle_location, v.insurance_annual_rate, v.insurance_daily_rate,
        v.insurance_rate_source, v.current_driver_customer_id, v.last_driver_customer_id,
        v.last_driver_annual_rate, v.status_changed_at, v.color, v.plate_status, v.plate_expiration,
        v.deal_status_raw, v.tlc_number || null, v.short_vin || null);
    }

    // Insert daily KPIs
    d.prepare(`INSERT INTO daily_kpis
      (snapshot_id, upload_date, total_vehicles, active_vehicles, vacant_vehicles,
       total_customers, active_customers, leads_count, daily_spend, money_on_floor,
       efficiency_score, tier_low, tier_med, tier_high, lead_tier_low, lead_tier_med, lead_tier_high)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`).run(
      snapId, uploadDate, kpis.total_vehicles, kpis.active_vehicles, kpis.vacant_vehicles,
      kpis.total_customers, kpis.active_customers, kpis.leads_count,
      kpis.daily_spend, kpis.money_on_floor, kpis.efficiency_score,
      kpis.tier_low, kpis.tier_med, kpis.tier_high,
      kpis.lead_tier_low, kpis.lead_tier_med, kpis.lead_tier_high
    );

    return snapId;
  });
  return txn();
}

// Get all snapshots (dates)
function getSnapshots() {
  return getDb().prepare("SELECT id, upload_date, uploaded_at, customers_count, vehicles_count, base_rate_annual FROM snapshots ORDER BY upload_date DESC").all();
}

// Get latest snapshot
function getLatestSnapshot() {
  return getDb().prepare("SELECT id, upload_date, uploaded_at, customers_count, vehicles_count, base_rate_annual FROM snapshots ORDER BY upload_date DESC LIMIT 1").get();
}

// Get customers for a snapshot
function getCustomers(snapshotId) {
  return getDb().prepare("SELECT * FROM customers WHERE snapshot_id = ? ORDER BY customer_id").all(snapshotId).map(row => ({
    ...row, has_active_deal: !!row.has_active_deal, is_lead: !!row.is_lead
  }));
}

// Get vehicles for a snapshot
function getVehicles(snapshotId) {
  return getDb().prepare("SELECT * FROM vehicles WHERE snapshot_id = ? ORDER BY vehicle_id").all(snapshotId).map(row => ({
    ...row, has_active_deal: !!row.has_active_deal
  }));
}

// Get daily KPIs for trend charts
function getDailyKpis(startDate, endDate) {
  return getDb().prepare(
    "SELECT * FROM daily_kpis WHERE upload_date BETWEEN ? AND ? ORDER BY upload_date"
  ).all(startDate, endDate);
}

// Get all daily KPIs
function getAllDailyKpis() {
  return getDb().prepare("SELECT * FROM daily_kpis ORDER BY upload_date").all();
}

// Settings
function getSetting(key, defaultVal) {
  const row = getDb().prepare("SELECT value FROM settings WHERE key = ?").get(key);
  return row ? row.value : defaultVal;
}

function setSetting(key, value) {
  getDb().prepare("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)").run(key, String(value));
}

module.exports = {
  getDb, saveSnapshot, getSnapshots, getLatestSnapshot,
  getCustomers, getVehicles, getDailyKpis, getAllDailyKpis,
  getSetting, setSetting,
};
