import express from "express";
import dotenv from "dotenv";
import getRawBody from "raw-body";
import crypto from "crypto";

dotenv.config();
const app = express();

console.log("MW UP", "secret_len", (process.env.SHOPIFY_WEBHOOK_SECRET || "").length);

// ---------- utils ----------
function hmacOk(raw, header, secret) {
  if (!header) return false;
  const sig = crypto.createHmac("sha256", secret).update(raw).digest("base64");
  const a = Buffer.from(header);
  const b = Buffer.from(sig);
  if (a.length !== b.length) return false;
  return crypto.timingSafeEqual(a, b);
}

function pickMark(order) {
  const note = String(order.note || "");
  const mT = note.match(/__MW_THEME\s*=\s*([^\s;]+)/i);
  const mD = note.match(/__MW_DEBUG\s*=\s*([^\s;]+)/i);
  if (mT) return { theme: mT[1], debug: !!(mD && mD[1].toLowerCase() === "on") };
  const attrs = Object.fromEntries((order.attributes || []).map(a => [a.name, a.value]));
  if (attrs.__MW_THEME) return { theme: attrs.__MW_THEME, debug: attrs.__MW_DEBUG === "on" };
  return null;
}

function toNum(x) {
  const n = Number.parseFloat(String(x || "0"));
  return Number.isFinite(n) ? n : 0;
}

function fmtShipDate(d = new Date()) {
  // MM/dd/yyyy HH:mm  (UTC)
  const dt = new Date(d);
  const pad = n => String(n).padStart(2, "0");
  const MM = pad(dt.getUTCMonth() + 1);
  const DD = pad(dt.getUTCDate());
  const YYYY = dt.getUTCFullYear();
  const hh = pad(dt.getUTCHours());
  const mm = pad(dt.getUTCMinutes());
  return `${MM}/${DD}/${YYYY} ${hh}:${mm}`;
}

function esc(x = "") {
  return String(x)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

function money2(v) {
  const n = Number.isFinite(v) ? v : 0;
  return n.toFixed(2);
}

function sum(items) {
  return items.reduce(
    (s, i) => s + Number(i.unitPrice || 0) * Number(i.qty || 0),
    0
  );
}

// ----- bundle helpers -----
function isBundleParent(li) {
  const p = li.properties || [];
  if (p.find(x => x.name === "_sb_parent" && String(x.value).toLowerCase() === "true")) return true;
  if (p.find(x => x.name === "_bundle" || x.name === "bundle_id")) return true;
  if (p.find(x => x.name === "skio_parent" && String(x.value).toLowerCase() === "true")) return true;
  return false;
}
function bundleKey(li) {
  const p = li.properties || [];
  const f1 = p.find(x => ["_sb_bundle_id","bundle_id","_bundle_id","skio_bundle_id"].includes(x.name));
  if (f1) return String(f1.value);
  const f2 = p.find(x => ["_sb_key","bundle_key","skio_bundle_key"].includes(x.name));
  return f2 ? String(f2.value) : null;
}

// ---------- transform ----------
function transformOrder(order) {
  const groups = {};
  for (const li of order.line_items) {
    const k = bundleKey(li);
    if (!k) continue;
    if (!groups[k]) groups[k] = { parent: null, children: [] };
    if (isBundleParent(li)) groups[k].parent = li; else groups[k].children.push(li);
  }

  const after = [];
  const handled = new Set();

  for (const [k, g] of Object.entries(groups)) {
    const parent = g.parent;
    const kids = g.children;
    if (parent) handled.add(parent.id);
    for (const c of kids) handled.add(c.id);

    if (parent && kids.length > 0) {
      const parentTotal = toNum(parent.price) * parent.quantity;
      let qtySum = 0; for (const c of kids) qtySum += c.quantity;

      after.push({ id: parent.id, title: parent.title, sku: parent.sku || null, qty: parent.quantity, unitPrice: 0, parent: true, key: k });

      if (parentTotal > 0 && qtySum > 0) {
        let rest = Math.round(parentTotal * 100);
        for (let i = 0; i < kids.length; i++) {
          const c = kids[i];
          const share = i === kids.length - 1 ? rest : Math.round((parentTotal * (c.quantity / qtySum)) * 100);
          rest -= share;
          const unit = c.quantity > 0 ? share / c.quantity / 100 : 0;
          after.push({ id: c.id, title: c.title, sku: c.sku || null, qty: c.quantity, unitPrice: Number(unit.toFixed(2)), parent: false, key: k });
        }
      } else {
        for (const c of kids) {
          after.push({ id: c.id, title: c.title, sku: c.sku || null, qty: c.quantity, unitPrice: toNum(c.price), parent: false, key: k });
        }
      }
    }
  }

  for (const li of order.line_items) {
    if (handled.has(li.id)) continue;
    after.push({ id: li.id, title: li.title, sku: li.sku || null, qty: li.quantity, unitPrice: toNum(li.price), parent: false, key: bundleKey(li) });
  }

  return { after };
}

// ---------- memory ----------
const history = [];
function remember(entry) { history.push(entry); while (history.length > 100) history.shift(); }
const last = () => (history.length ? history[history.length - 1] : null);

// ---------- webhook ----------
app.post("/webhooks/orders-create", async (req, res) => {
  const raw = await getRawBody(req);
  const hdr = req.headers["x-shopify-hmac-sha256"] || "";
  const ok = hdr && hmacOk(raw, hdr, process.env.SHOPIFY_WEBHOOK_SECRET || "");
  if (!ok) { res.status(401).send("bad hmac"); return; }

  const order = JSON.parse(raw.toString("utf8"));
  const mark = pickMark(order);
  if (!mark || !String(mark.theme || "").startsWith("preview-")) { console.log("skip", order.id, order.name); res.status(200).send("skip"); return; }

  const conv = transformOrder(order);
  remember({
    id: order.id, name: order.name,
    currency: order.currency, total_price: toNum(order.total_price),
    email: order.email || "",
    shipping_address: order.shipping_address || {},
    billing_address: order.billing_address || {},
    payload: conv,
    created_at: new Date().toISOString()
  });

  console.log("MATCH", order.id, order.name, "items:", conv.after.length);
  res.status(200).send("ok");
});

// ---------- XML helpers ----------
function skuSafe(i, orderId) {
  const s = (i.sku || "").trim();
  if (s) return s;
  const base = i.id ? String(i.id) : (i.title ? i.title.replace(/\s+/g, "-").slice(0, 24) : "ITEM");
  return `MW-${orderId}-${base}`;
}

function normState(s) {
  const v = (s || "").trim();
  if (!v) return "ST";
  return v.length === 2 ? v : v.slice(0, 2).toUpperCase();
}

function filledAddress(a){
  const d = v => (v && String(v).trim()) ? String(v) : "";
  const name = [d(a.first_name), d(a.last_name)].filter(Boolean).join(" ") || "Customer";
  return {
    name,
    company: d(a.company) || "",
    phone: d(a.phone) || "",
    email: d(a.email) || "",
    address1: d(a.address1) || "Address line 1",
    address2: d(a.address2) || "",
    city:    d(a.city) || "City",
    state:   normState(a.province_code || a.province),
    zip:     d(a.zip) || "00000",
    country: (d(a.country_code) || "US").slice(0,2).toUpperCase()
  };
}

function minimalXML(o){
  if (!o) {
    return `<?xml version="1.0" encoding="utf-8"?>
<Orders>
</Orders>`;
  }

  // children items = только реальные (parent=false)
  let children = (o.payload?.after || []).filter(i => !i.parent);
  if (!children.length) {
    children = [{ id: "FALLBACK", title: "Bundle", sku: "BUNDLE", qty: 1, unitPrice: Number(o.total_price || 0) }];
  }

  // суммы
  const subtotal = sum(children);
  const tax = 0;
  const shipping = 0;
  const total = subtotal + tax + shipping;

  // адреса
  const bill = filledAddress(o.billing_address || {});
  const ship = filledAddress(o.shipping_address || {});
  const email = (o.email && o.email.includes("@")) ? o.email : "customer@example.com";
  const orderDate = fmtShipDate(new Date(o.created_at || Date.now()));
  const lastMod   = fmtShipDate(new Date());

  const itemsXml = children.map(i => `
      <Item>
        <LineItemID>${esc(String(i.id || ""))}</LineItemID>
        <SKU>${esc(skuSafe(i, o.id))}</SKU>
        <Name>${esc(i.title || "Item")}</Name>
        <Quantity>${Math.max(1, parseInt(i.qty || 0, 10))}</Quantity>
        <UnitPrice>${money2(Number.isFinite(i.unitPrice) ? i.unitPrice : 0)}</UnitPrice>
        <Adjustment>false</Adjustment>
      </Item>`).join("");

  // структура точно по референс-схеме
  return `<?xml version="1.0" encoding="utf-8"?>
<Orders>
  <Order>
    <OrderID>${esc(String(o.id))}</OrderID>
    <OrderNumber>${esc(o.name || String(o.id))}</OrderNumber>
    <OrderDate>${orderDate}</OrderDate>
    <OrderStatus>awaiting_shipment</OrderStatus>
    <LastModified>${lastMod}</LastModified>

    <ShippingMethod>Ground</ShippingMethod>
    <PaymentMethod>Other</PaymentMethod>
    <CurrencyCode>${esc(o.currency || "USD")}</CurrencyCode>

    <OrderTotal>${money2(total)}</OrderTotal>
    <TaxAmount>${money2(tax)}</TaxAmount>
    <ShippingAmount>${money2(shipping)}</ShippingAmount>

    <Customer>
      <CustomerCode>${esc(email)}</CustomerCode>
      <BillTo>
        <Name>${esc(bill.name)}</Name>
        <Company>${esc(bill.company)}</Company>
        <Phone>${esc(bill.phone)}</Phone>
        <Email>${esc(email)}</Email>
        <Address1>${esc(bill.address1)}</Address1>
        <Address2>${esc(bill.address2)}</Address2>
        <City>${esc(bill.city)}</City>
        <State>${esc(bill.state)}</State>
        <PostalCode>${esc(bill.zip)}</PostalCode>
        <Country>${esc(bill.country)}</Country>
      </BillTo>
      <ShipTo>
        <Name>${esc(ship.name)}</Name>
        <Company>${esc(ship.company)}</Company>
        <Address1>${esc(ship.address1)}</Address1>
        <Address2>${esc(ship.address2)}</Address2>
        <City>${esc(ship.city)}</City>
        <State>${esc(ship.state)}</State>
        <PostalCode>${esc(ship.zip)}</PostalCode>
        <Country>${esc(ship.country)}</Country>
        <Phone>${esc(ship.phone)}</Phone>
      </ShipTo>
    </Customer>

    <Items>${itemsXml}
    </Items>
  </Order>
</Orders>`;
}

// sample на время коннекта/импорта
function buildSampleOrder(){
  const now = new Date().toISOString();
  return {
    id: 999000111,
    name: "#SAMPLE",
    currency: "USD",
    email: "sample@mycolean.com",
    shipping_address: {
      first_name: "Sample", last_name: "Buyer",
      address1: "1 Sample Street", address2: "",
      city: "Austin", province_code: "TX", zip: "73301", country_code: "US", phone: ""
    },
    billing_address: {
      first_name: "Sample", last_name: "Buyer",
      address1: "1 Sample Street", address2: "",
      city: "Austin", province_code: "TX", zip: "73301", country_code: "US", phone: ""
    },
    payload: {
      after: [
        { id: "S1", title: "Mycolean Classic 4-Pack", sku: "MYCO-4PK", qty: 1, unitPrice: 49.95, parent: false },
        { id: "S2", title: "Mystery Bottle", sku: "MYCO-MYST", qty: 1, unitPrice: 0.00, parent: false }
      ]
    },
    total_price: 49.95,
    created_at: now
  };
}

// ---------- auth ----------
function authOK(req){
  const h = req.headers.authorization || "";
  if (h.startsWith("Basic ")) {
    const [u, p] = Buffer.from(h.slice(6), "base64").toString("utf8").split(":", 2);
    if (u === process.env.SS_USER && p === process.env.SS_PASS) return true;
  }
  const q = req.query || {};
  const uq = q["SS-UserName"] || q["ss-username"] || q["SS-USERNAME"] || q["username"];
  const pq = q["SS-Password"] || q["ss-password"] || q["SS-PASSWORD"] || q["password"];
  if (uq && pq && String(uq) === process.env.SS_USER && String(pq) === process.env.SS_PASS) return true;
  return false;
}
function logSS(req, tag){ console.log(`[SS] ${tag}`, req.method, req.originalUrl, req.headers["user-agent"]||""); }

// ---------- ShipStation endpoint ----------
function shipstationHandler(req, res) {
  logSS(req,"hit");
  res.set({
    "Content-Type": "application/xml; charset=utf-8",
    "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
    "Pragma": "no-cache",
    "Expires": "0"
  });

  if (!process.env.SS_USER || !process.env.SS_PASS) { res.status(503).send(`<?xml version="1.0" encoding="utf-8"?><Error>Auth not configured</Error>`); return; }
  if (!authOK(req)) { res.status(401).set("WWW-Authenticate","Basic").send(`<?xml version="1.0" encoding="utf-8"?><Error>Auth</Error>`); return; }

  if (req.method.toUpperCase() === "HEAD") { res.status(200).end(); return; }

  const q = Object.fromEntries(Object.entries(req.query).map(([k,v])=>[k.toLowerCase(), String(v)]));
  const action = (q.action || "").toLowerCase();

  if (action === "test" || action === "status") {
    const xml = `<?xml version="1.0" encoding="utf-8"?><Store><Status>OK</Status></Store>`;
    console.log("[SS] xml(test):", xml.slice(0, 200));
    res.status(200).send(xml);
    return;
  }

  let o = last();

  if (!o && String(process.env.SS_SAMPLE_ON_EMPTY || "").toLowerCase() === "true") {
    o = buildSampleOrder();
    console.log("[SS] using SAMPLE order");
  }

  const strictDates = String(process.env.SS_STRICT_DATES || "").toLowerCase() === "true";
  if (o && strictDates) {
    const start = q.start_date ? Date.parse(q.start_date) : null;
    const end   = q.end_date   ? Date.parse(q.end_date)   : null;
    if (start || end) {
      const created = Date.parse(o.created_at || new Date().toISOString());
      if ((start && created < start) || (end && created > end)) {
        console.log("[SS] filtered out by date window", { created, start, end });
        o = null;
      }
    }
  }

  const xml = minimalXML(o);
  console.log("[SS] xml(export):", xml.slice(0, 300).replace(/\s+/g,' ').trim());
  res.status(200).send(xml);
}

app.get("/shipstation", shipstationHandler);
app.post("/shipstation", shipstationHandler);
app.head("/shipstation", shipstationHandler);

// ---------- health ----------
app.get("/health", (req,res)=>res.send("ok"));

// ---------- start ----------
app.listen(process.env.PORT || 8080);
