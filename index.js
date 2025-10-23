// index.js
const express = require("express");
const crypto = require("crypto");
const qs = require("querystring");

const app = express();

// ---------- helpers ----------
const nowIso = () => new Date().toISOString();
const toNum = v => Number(parseFloat(v || 0).toFixed(2));
const esc = s =>
  String(s == null ? "" : s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");

const log = (...a) => console.log(new Date().toTimeString().slice(0, 8), ...a);

// ---------- raw body for HMAC ----------
app.use((req, res, next) => {
  if (req.path.startsWith("/webhooks/")) {
    let chunks = [];
    req.on("data", c => chunks.push(c));
    req.on("end", () => {
      req.rawBody = Buffer.concat(chunks);
      try {
        req.body = JSON.parse(req.rawBody.toString("utf8"));
      } catch (_) {
        req.body = {};
      }
      next();
    });
  } else {
    express.json({ limit: "2mb" })(req, res, next);
  }
});

// ---------- in-memory store ----------
const history = [];           // массив последних N
const statusById = new Map(); // id -> 'open'|'cancelled'

const remember = (order) => {
  const obj = {
    id: order.id,
    name: order.name || `#${order.id}`,
    currency: order.currency || "USD",
    email: (order.customer && order.customer.email) || order.email || "",
    shipping_address: order.shipping_address || {},
    billing_address: order.billing_address || {},
    line_items: order.line_items || [],
    note: order.note || "",
    created_at: order.created_at || nowIso(),
    total_price: toNum(order.total_price || 0),
    payload: { before: order.line_items || [], after: [] }
  };
  // начальный статус
  if (!statusById.has(order.id)) statusById.set(order.id, "open");

  // трансформация
  obj.payload.after = transformOrder(obj).after;

  // сохраняем в историю
  const idx = history.findIndex(o => o.id === obj.id);
  if (idx >= 0) history[idx] = obj;
  else history.unshift(obj);
  if (history.length > 300) history.pop();

  log("MATCH", obj.id, obj.name, "items:", obj.payload.after.length);
  return obj;
};

// ---------- shopify HMAC ----------
const verifyHmac = (req) => {
  const secret = process.env.SHOPIFY_SHARED_SECRET || "";
  if (!secret) return false;
  const header = req.get("X-Shopify-Hmac-Sha256") || "";
  if (!header) return false;
  const digest = crypto
    .createHmac("sha256", secret)
    .update(req.rawBody || Buffer.from(""))
    .digest("base64");
  try {
    return crypto.timingSafeEqual(Buffer.from(header), Buffer.from(digest));
  } catch {
    return header === digest;
  }
};

// ---------- SKIO/bundle transform + MOCK ----------
function transformOrder(order) {
  const after = [];
  const groups = {}; // key -> { parent, children[] }

  const items = order.line_items || [];

  // try detect bundle/skio by properties
  for (const li of items) {
    const props = li.properties || [];
    const map = {};
    for (const p of props) {
      if (p && p.name) map[p.name] = p.value;
    }

    const isChildOfBundle =
      map._bundle_parent || map._bundle || map._sb_parent || map.bundle_parent;
    const isSkioChild = map.skio_parent || map.skio_bundle_id;

    if (isChildOfBundle || isSkioChild) {
      const key =
        String(map._bundle_parent || map._sb_parent || map.skio_parent || map.skio_bundle_id || "grp") +
        ":" +
        String(li.parent_id || li.product_id || "x");
      if (!groups[key]) groups[key] = { parent: null, children: [] };
      groups[key].children.push(li);
    } else if (map._bundle_root || map._sb_root || map.skio_root) {
      const key =
        String(map._bundle_root || map._sb_root || map.skio_root || "grp") +
        ":" +
        String(li.id || li.product_id || "x");
      if (!groups[key]) groups[key] = { parent: null, children: [] };
      groups[key].parent = li;
    } else {
      after.push({
        id: li.id,
        title: li.title,
        sku: li.sku || null,
        qty: li.quantity || 1,
        unitPrice: toNum(li.price),
        parent: false,
        key: null
      });
    }
  }

  // collapse groups: parent 0, children priced
  for (const key of Object.keys(groups)) {
    const g = groups[key];
    if (g.parent) {
      after.push({
        id: g.parent.id,
        title: g.parent.title + " (PARENT)",
        sku: g.parent.sku || null,
        qty: g.parent.quantity || 1,
        unitPrice: 0,
        parent: true,
        key
      });
    }
    for (const c of g.children) {
      after.push({
        id: c.id,
        title: c.title,
        sku: c.sku || null,
        qty: c.quantity || 1,
        unitPrice: toNum(c.price),
        parent: false,
        key
      });
    }
  }

  // MOCK подписки через note
  const noteStr = String(order.note || "");
  const noGroups = Object.keys(groups).length === 0;
  if (noGroups && /subscription_test\s*=\s*yes/i.test(noteStr)) {
    const li = items[0];
    if (li) {
      const qty = li.quantity || 1;
      const unit = toNum(li.price);
      after.length = 0;
      after.push({
        id: li.id,
        title: li.title + " (SUB PARENT)",
        sku: li.sku || null,
        qty,
        unitPrice: 0,
        parent: true,
        key: "mock-sub"
      });
      after.push({
        id: li.id + "-child",
        title: li.title + " (SUB CHILD)",
        sku: (li.sku ? li.sku + "-CHILD" : "SUB-CHILD"),
        qty,
        unitPrice: unit,
        parent: false,
        key: "mock-sub"
      });
    }
  }

  return { after };
}

// ---------- shopify webhooks ----------
app.post("/webhooks/orders-create", (req, res) => {
  const ok = verifyHmac(req);
  log("WH HIT", nowIso(), "hmac:", ok ? "present" : "bad", "secret_len:", (process.env.SHOPIFY_SHARED_SECRET || "").length);
  if (!ok) return res.status(401).send("BAD HMAC");

  try {
    const o = req.body || {};
    statusById.set(o.id, "open");
    remember(o);
    return res.status(200).send("OK");
  } catch (e) {
    console.error(e);
    return res.status(500).send("ERR");
  }
});

app.post("/webhooks/orders-cancelled", (req, res) => {
  const ok = verifyHmac(req);
  log("WH HIT", nowIso(), "hmac:", ok ? "present" : "bad", "secret_len:", (process.env.SHOPIFY_SHARED_SECRET || "").length);
  if (!ok) return res.status(401).send("BAD HMAC");
  try {
    const o = req.body || {};
    statusById.set(o.id, "cancelled");
    remember(o);
    log("cancel match", o.id, o.name);
    return res.status(200).send("OK");
  } catch (e) {
    console.error(e);
    return res.status(500).send("ERR");
  }
});

// ---------- ShipStation endpoint ----------
function basicOk(req) {
  const u = process.env.SS_USER || "";
  const p = process.env.SS_PASS || "";
  // support query creds (SS-UserName & SS-Password)
  const q = req.query || {};
  if (q["SS-UserName"] && q["SS-Password"]) {
    return q["SS-UserName"] === u && q["SS-Password"] === p;
  }
  const h = req.get("authorization") || "";
  if (h.startsWith("Basic ")) {
    const b = Buffer.from(h.slice(6), "base64").toString("utf8");
    const [uu, pp] = b.split(":");
    return uu === u && pp === p;
  }
  return false;
}

const fmtDate = (d = new Date()) => {
  // MM/dd/yyyy HH:mm (24h)
  const pad = x => (x < 10 ? "0" + x : String(x));
  const dt = new Date(d);
  return `${pad(dt.getMonth() + 1)}/${pad(dt.getDate())}/${dt.getFullYear()} ${pad(dt.getHours())}:${pad(dt.getMinutes())}`;
};

function minimalXML(o) {
  const status = statusById.get(o.id) === "cancelled" ? "cancelled" : "awaiting_shipment";
  const ship = o.shipping_address || {};
  const bill = o.billing_address || {};
  const items = (o.payload && o.payload.after) || [];

  const addressNode = (node, isShip) => `
      <Name>${esc(node.name || `${node.first_name || ""} ${node.last_name || ""}`.trim())}</Name>
      <Company>${esc(node.company || "")}</Company>
      <Address1>${esc(node.address1 || "")}</Address1>
      <Address2>${esc(node.address2 || "")}</Address2>
      <City>${esc(node.city || "")}</City>
      <State>${esc(node.province || "")}</State>
      <PostalCode>${esc(node.zip || "")}</PostalCode>
      <Country>${esc((node.country_code || node.country || "").toString().slice(0,2).toUpperCase())}</Country>
      ${isShip ? `<Phone>${esc(node.phone || "")}</Phone>` : ""}`;

  const itemsXml = items.map(li => `
      <Item>
        <LineItemID>${esc(String(li.id))}</LineItemID>
        <SKU>${esc(li.sku || "")}</SKU>
        <Name>${esc(li.title || "")}</Name>
        <Quantity>${esc(li.qty || 1)}</Quantity>
        <UnitPrice>${esc(li.unitPrice != null ? li.unitPrice : 0)}</UnitPrice>
        <Adjustment>${li.parent ? "true" : "false"}</Adjustment>
      </Item>`).join("");

  const orderTotal = items.reduce((a, b) => a + toNum(b.unitPrice) * (b.qty || 1), 0);

  return `
  <Order>
    <OrderID>${esc(String(o.id))}</OrderID>
    <OrderNumber>${esc(o.name || String(o.id))}</OrderNumber>
    <OrderDate>${esc(fmtDate(new Date(o.created_at || Date.now())))}</OrderDate>
    <LastModified>${esc(fmtDate(new Date()))}</LastModified>
    <OrderStatus>${esc(status)}</OrderStatus>
    <ShippingMethod>${esc("Ground")}</ShippingMethod>
    <PaymentMethod>${esc("Other")}</PaymentMethod>
    <CurrencyCode>${esc(o.currency || "USD")}</CurrencyCode>
    <OrderTotal>${esc(orderTotal.toFixed(2))}</OrderTotal>
    <TaxAmount>0.00</TaxAmount>
    <ShippingAmount>0.00</ShippingAmount>
    <Customer>
      <CustomerCode>${esc(o.email || "customer@example.com")}</CustomerCode>
      <BillTo>${addressNode(bill, false)}</BillTo>
      <ShipTo>${addressNode(ship, true)}</ShipTo>
    </Customer>
    <Items>${itemsXml}</Items>
  </Order>`;
}

app.get("/shipstation", (req, res) => {
  log("[SS] hit GET", req.url, "ShipStation");
  if (req.query.action === "test") {
    res.set("Content-Type", "application/xml; charset=utf-8");
    return res.send(`<?xml version="1.0" encoding="utf-8"?><Store><Status>OK</Status></Store>`);
  }
  if (!basicOk(req)) return res.status(401).set("WWW-Authenticate", "Basic").send("auth");

  // window by dates
  const strict = String(process.env.SS_STRICT_DATES || "").toLowerCase() === "true";
  let start = req.query.start_date ? decodeURIComponent(req.query.start_date) : null;
  let end = req.query.end_date ? decodeURIComponent(req.query.end_date) : null;

  let startTs = 0, endTs = Date.now();
  if (strict && start) {
    const s = new Date(start.replace(/%2f/gi,"/").replace(/\+/g," "));
    if (!isNaN(s)) startTs = s.getTime();
  }
  if (strict && end) {
    const e = new Date(end.replace(/%2f/gi,"/").replace(/\+/g," "));
    if (!isNaN(e)) endTs = e.getTime();
  }

  const list = history.filter(o => {
    const t = new Date(o.created_at || o.updated_at || Date.now()).getTime();
    if (strict) return t >= startTs && t <= endTs;
    return true;
  });

  const body = `<?xml version="1.0" encoding="utf-8"?>
<Orders total="${list.length}" page="1" pages="1">
  <Total>${list.length}</Total>
  <Page>1</Page>
  <Pages>1</Pages>
  ${list.map(minimalXML).join("\n")}
</Orders>`;

  log("[SS] xml(export):", body.substring(0, 300).replace(/\s+/g, " "));
  res.set("Content-Type", "application/xml; charset=utf-8");
  res.set("Cache-Control", "no-store");
  res.send(body);
});

// ---------- admin: force-cancel ----------
app.post("/admin/mark-cancelled", (req, res) => {
  const token = req.get("x-admin-token") || req.query.token || "";
  if (!process.env.ADMIN_TOKEN || token !== process.env.ADMIN_TOKEN) {
    return res.status(401).json({ ok: false, error: "unauthorized" });
  }
  const { orderNumber } = req.body || {};
  const num = String(orderNumber || req.query.orderNumber || "").trim();
  if (!num) return res.status(400).json({ ok: false, error: "orderNumber required" });

  let o = history.find(x => x.name === num);
  if (!o) {
    o = {
      id: `legacy-${num.replace(/[^0-9]/g, "") || Date.now()}`,
      name: num,
      currency: "USD",
      email: "customer@example.com",
      shipping_address: {},
      billing_address: {},
      line_items: [],
      note: "",
      created_at: nowIso(),
      total_price: 0,
      payload: { before: [], after: [] }
    };
    remember(o);
  }
  statusById.set(o.id, "cancelled");
  log("[ADMIN] force-cancel", o.id, o.name);
  return res.json({ ok: true, id: o.id, name: o.name, status: "cancelled" });
});

// ---------- health ----------
app.get("/", (_req, res) => res.type("text").send("ok"));

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  log("Your service is live");
  log("Detected service running on port", PORT);
});
