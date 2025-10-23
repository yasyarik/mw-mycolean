// index.js (ESM, works with "type":"module")

import express from "express";
import crypto from "crypto";

const app = express();

/* ========= helpers ========= */
const nowIso = () => new Date().toISOString();
const toNum = v => {
  const n = Number.parseFloat(v);
  return Number.isFinite(n) ? Number(n.toFixed(2)) : 0;
};
const esc = s =>
  String(s == null ? "" : s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");

const log = (...a) => console.log(new Date().toISOString(), ...a);

const fmtDateShip = (d = new Date()) => {
  // MM/dd/yyyy HH:mm (UTC)
  const t = new Date(d);
  const pad = n => String(n).padStart(2, "0");
  return `${pad(t.getUTCMonth() + 1)}/${pad(t.getUTCDate())}/${t.getUTCFullYear()} ${pad(
    t.getUTCHours()
  )}:${pad(t.getUTCMinutes())}`;
};

/* ========= raw body for webhooks (HMAC) ========= */
app.use((req, res, next) => {
  if (req.path.startsWith("/webhooks/")) {
    const chunks = [];
    req.on("data", c => chunks.push(c));
    req.on("end", () => {
      req.rawBody = Buffer.concat(chunks);
      try {
        req.body = JSON.parse(req.rawBody.toString("utf8"));
      } catch {
        req.body = {};
      }
      next();
    });
  } else {
    express.json({ limit: "2mb" })(req, res, next);
  }
});

const verifyHmac = req => {
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

/* ========= in-memory storage ========= */
const history = []; // последние N заказов (для экспорта)
const statusById = new Map(); // order.id -> 'awaiting_shipment' | 'cancelled'

const last = () => (history.length ? history[0] : null);

function remember(orderLike) {
  const o = {
    id: orderLike.id,
    name: orderLike.name || `#${orderLike.id}`,
    currency: orderLike.currency || "USD",
    email: (orderLike.customer && orderLike.customer.email) || orderLike.email || "",
    shipping_address: orderLike.shipping_address || {},
    billing_address: orderLike.billing_address || {},
    line_items: orderLike.line_items || [],
    note: orderLike.note || "",
    created_at: orderLike.created_at || nowIso(),
    total_price: toNum(orderLike.total_price || 0),
    payload: { after: [] }
  };

  // трансформация
  o.payload.after = transformOrder(o).after;

  // сохранить
  const idx = history.findIndex(x => x.id === o.id);
  if (idx >= 0) history.splice(idx, 1);
  history.unshift(o);
  while (history.length > 200) history.pop();

  if (!statusById.has(o.id)) statusById.set(o.id, "awaiting_shipment");

  log("MATCH", o.id, o.name, "items:", o.payload.after.length);
  return o;
}

/* ========= bundle/SKIO transform ========= */

function isBundleParentProps(propsMap) {
  return (
    String(propsMap._sb_parent).toLowerCase() === "true" ||
    String(propsMap._bundle_root).toLowerCase() === "true" ||
    String(propsMap.skio_parent).toLowerCase() === "true" ||
    String(propsMap.skio_root).toLowerCase() === "true"
  );
}
function isBundleChildProps(propsMap) {
  return (
    String(propsMap._bundle) ||
    String(propsMap.bundle_id) ||
    String(propsMap._sb_child) ||
    String(propsMap.skio_bundle_id)
  );
}
function propsKey(li) {
  const p = Object.fromEntries((li.properties || []).map(p => [p.name, p.value]));
  return (
    String(
      p._sb_bundle_id ||
        p.bundle_id ||
        p._bundle_id ||
        p.skio_bundle_id ||
        p._sb_key ||
        p.bundle_key ||
        p.skio_bundle_key ||
        ""
    ) || null
  );
}

function transformOrder(order) {
  const items = order.line_items || [];
  const groups = {}; // key -> { parent, children[] }
  const after = [];
  const handled = new Set();

  const mapProps = li => Object.fromEntries((li.properties || []).map(p => [p.name, p.value]));

  // группировка
  for (const li of items) {
    const props = mapProps(li);
    const key = propsKey(li);
    if (!key) continue;

    if (!groups[key]) groups[key] = { parent: null, children: [] };

    if (isBundleParentProps(props)) {
      groups[key].parent = li;
    } else if (isBundleChildProps(props)) {
      groups[key].children.push(li);
    }
  }

  // перенести сгруппированные
  for (const [key, g] of Object.entries(groups)) {
    const parent = g.parent;
    const kids = g.children;

    if (parent) handled.add(parent.id);
    for (const c of kids) handled.add(c.id);

    if (parent) {
      after.push({
        id: parent.id,
        title: parent.title,
        sku: parent.sku || null,
        qty: parent.quantity || 1,
        unitPrice: toNum(parent.price),
        parent: true,
        key
      });
    }

    for (const c of kids) {
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

  // остальные (несгруппированные)
  for (const li of items) {
    if (handled.has(li.id)) continue;
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

  // SKIO-логика: если цена на родителе, а у детей 0 — раздать цену родителя по детям (по qty)
  for (const key of new Set(after.map(x => x.key).filter(Boolean))) {
    const groupLines = after.filter(x => x.key === key);
    const parentLine = groupLines.find(x => x.parent);
    const childLines = groupLines.filter(x => !x.parent);

    if (!parentLine || childLines.length === 0) continue;

    const parentHasPrice = toNum(parentLine.unitPrice) > 0;
    const childrenAllZero = childLines.every(c => toNum(c.unitPrice) === 0);

    if (parentHasPrice && childrenAllZero) {
      const parentTotal = toNum(parentLine.unitPrice) * (parentLine.qty || 1);
      const totalChildQty = childLines.reduce((a, c) => a + (c.qty || 1), 0) || 1;

      let rest = Math.round(parentTotal * 100);
      for (let i = 0; i < childLines.length; i++) {
        const c = childLines[i];
        const share =
          i === childLines.length - 1
            ? rest
            : Math.round(((parentTotal * (c.qty || 1)) / totalChildQty) * 100);
        rest -= share;
        const unit = (c.qty || 1) > 0 ? share / (c.qty || 1) / 100 : 0;
        c.unitPrice = Number(unit.toFixed(2));
      }
      parentLine.unitPrice = 0;
    }
  }

  // MOCK подписки по note: "subscription_test=yes"
  const noteStr = String(order.note || "");
  const hasAnyGroup = after.some(x => x.key);
  if (!hasAnyGroup && /subscription_test\s*=\s*yes/i.test(noteStr) && after.length > 0) {
    const li = after[0];
    const total = toNum(li.unitPrice) * (li.qty || 1);
    after.length = 0;
    after.push({
      id: String(li.id),
      title: li.title + " (SUB PARENT)",
      sku: li.sku || null,
      qty: li.qty || 1,
      unitPrice: 0,
      parent: true,
      key: "mock-sub"
    });
    after.push({
      id: String(li.id) + "-child",
      title: li.title + " (SUB CHILD)",
      sku: li.sku ? li.sku + "-CHILD" : "SUB-CHILD",
      qty: li.qty || 1,
      unitPrice: Number((total / (li.qty || 1)).toFixed(2)),
      parent: false,
      key: "mock-sub"
    });
  }

  return { after };
}

/* ========= Shopify webhooks ========= */

app.post("/webhooks/orders-create", (req, res) => {
  const ok = verifyHmac(req);
  if (!ok) return res.status(401).send("BAD HMAC");
  const order = req.body || {};
  const note = String(order.note || "");
  const themeMark = /__MW_THEME\s*=\s*preview-/i.test(note);
  if (!themeMark) {
    log("skip (no preview mark)", order.id, order.name);
    return res.status(200).send("OK");
  }
  statusById.set(order.id, "awaiting_shipment");
  remember(order);
  return res.status(200).send("OK");
});

app.post("/webhooks/orders-cancelled", (req, res) => {
  const ok = verifyHmac(req);
  if (!ok) return res.status(401).send("BAD HMAC");
  const order = req.body || {};
  const note = String(order.note || "");
  const themeMark = /__MW_THEME\s*=\s*preview-/i.test(note);
  if (!themeMark) {
    log("cancel skip (no preview mark)", order.id, order.name);
    return res.status(200).send("OK");
  }
  statusById.set(order.id, "cancelled");
  if (!history.find(x => x.id === order.id)) remember(order);
  log("CANCEL MATCH", order.id, order.name);
  return res.status(200).send("OK");
});

/* ========= ShipStation endpoint ========= */

function basicOk(req) {
  const u = process.env.SS_USER || "";
  const p = process.env.SS_PASS || "";
  if (!u || !p) return false;

  const q = req.query || {};
  if (q["SS-UserName"] && q["SS-Password"]) {
    return String(q["SS-UserName"]) === u && String(q["SS-Password"]) === p;
    }
  const h = req.get("authorization") || "";
  if (h.startsWith("Basic ")) {
    const [uu, pp] = Buffer.from(h.slice(6), "base64").toString("utf8").split(":", 2);
    return uu === u && pp === p;
  }
  return false;
}

function skuSafe(item, orderId) {
  const s = String(item.sku || "").trim();
  if (s) return s;
  const base = item.id ? String(item.id) : (item.title ? item.title.replace(/\s+/g, "-").slice(0, 24) : "ITEM");
  return `MW-${orderId}-${base}`;
}

function filledAddress(a = {}) {
  const name =
    a.name ||
    [a.first_name, a.last_name].filter(Boolean).join(" ") ||
    "Customer";
  const country = (a.country_code || a.country || "US").toString().slice(0, 2).toUpperCase();
  const state = (a.province_code || a.province || "ST").toString().slice(0, 2).toUpperCase();
  return {
    name,
    company: a.company || "",
    address1: a.address1 || "Address line 1",
    address2: a.address2 || "",
    city: a.city || "City",
    state,
    zip: a.zip || "00000",
    country,
    phone: a.phone || ""
  };
}

function buildOrderXML(o) {
  const status = statusById.get(o.id) || "awaiting_shipment";

  let children = (o.payload && o.payload.after ? o.payload.after.filter(i => !i.parent) : []);
  if (!children.length) {
    children = [{ id: "FALLBACK", title: "Item", sku: "ITEM", qty: 1, unitPrice: toNum(o.total_price || 0), parent: false }];
  }

  const subtotal = children.reduce((s, i) => s + toNum(i.unitPrice) * (i.qty || 1), 0);
  const tax = 0;
  const shipping = 0;
  const total = subtotal + tax + shipping;

  const bill = filledAddress(o.billing_address);
  const ship = filledAddress(o.shipping_address);
  const email = (o.email && o.email.includes("@")) ? o.email : "customer@example.com";

  const itemsXml = children.map(i => `
      <Item>
        <LineItemID>${esc(String(i.id || ""))}</LineItemID>
        <SKU>${esc(skuSafe(i, o.id))}</SKU>
        <Name>${esc(i.title || "Item")}</Name>
        <Quantity>${Math.max(1, parseInt(i.qty || 0, 10))}</Quantity>
        <UnitPrice>${toNum(i.unitPrice).toFixed(2)}</UnitPrice>
        <Adjustment>false</Adjustment>
      </Item>`).join("");

  return `
  <Order>
    <OrderID>${esc(String(o.id))}</OrderID>
    <OrderNumber>${esc(o.name || String(o.id))}</OrderNumber>
    <OrderDate>${esc(fmtDateShip(new Date(o.created_at || Date.now())))}</OrderDate>
    <OrderStatus>${esc(status)}</OrderStatus>
    <LastModified>${esc(fmtDateShip(new Date()))}</LastModified>
    <ShippingMethod>Ground</ShippingMethod>
    <PaymentMethod>Other</PaymentMethod>
    <CurrencyCode>${esc(o.currency || "USD")}</CurrencyCode>
    <OrderTotal>${total.toFixed(2)}</OrderTotal>
    <TaxAmount>${tax.toFixed(2)}</TaxAmount>
    <ShippingAmount>${shipping.toFixed(2)}</ShippingAmount>
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
  </Order>`;
}

app.get("/shipstation", (req, res) => {
  if ((req.query.action || "").toLowerCase() === "test") {
    res.type("application/xml").send(`<?xml version="1.0" encoding="utf-8"?><Store><Status>OK</Status></Store>`);
    return;
  }
  if (!basicOk(req)) {
    return res.status(401).set("WWW-Authenticate", "Basic").type("application/xml").send(`<?xml version="1.0" encoding="utf-8"?><Error>Auth</Error>`);
  }

  const strict = String(process.env.SS_STRICT_DATES || "").toLowerCase() === "true";
  let list = [...history];

  if (strict) {
    const q = req.query || {};
    const s = q.start_date ? Date.parse(q.start_date) : null;
    const e = q.end_date ? Date.parse(q.end_date) : null;
    if (s || e) {
      list = list.filter(o => {
        const t = Date.parse(o.created_at || nowIso());
        if (s && t < s) return false;
        if (e && t > e) return false;
        return true;
      });
    }
  }

  const xml = `<?xml version="1.0" encoding="utf-8"?>
<Orders>
${list.map(buildOrderXML).join("\n")}
</Orders>`;
  log("[SS] export", `count=${list.length}`);
  res.set({
    "Content-Type": "application/xml; charset=utf-8",
    "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
    "Pragma": "no-cache",
    "Expires": "0"
  });
  res.send(xml);
});

/* ========= health ========= */
app.get("/", (_req, res) => res.type("text/plain").send("ok"));

/* ========= start ========= */
const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  log("MW listening on", PORT);
});

/* ========= helpers used by shipstation auth ========= */
function basicOk(req) {
  const u = process.env.SS_USER || "";
  const p = process.env.SS_PASS || "";
  if (!u || !p) return false;

  const q = req.query || {};
  if (q["SS-UserName"] && q["SS-Password"]) {
    return String(q["SS-UserName"]) === u && String(q["SS-Password"]) === p;
  }
  const h = req.get("authorization") || "";
  if (h.startsWith("Basic ")) {
    const [uu, pp] = Buffer.from(h.slice(6), "base64").toString("utf8").split(":", 2);
    return uu === u && pp === p;
  }
  return false;
}
