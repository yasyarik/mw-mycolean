import express from "express";
import dotenv from "dotenv";
import getRawBody from "raw-body";
import crypto from "crypto";

dotenv.config();
const app = express();

console.log("MW UP", "secret_len", (process.env.SHOPIFY_WEBHOOK_SECRET || "").length);

// ========== utils ==========
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
function toNum(x) { const n = Number.parseFloat(String(x || "0")); return Number.isFinite(n) ? n : 0; }

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

// ========== transform ==========
function transformOrder(order) {
  const before = order.line_items.map(li => ({
    id: li.id, title: li.title, sku: li.sku || null, qty: li.quantity,
    price: toNum(li.price), total: toNum(li.price) * li.quantity,
    key: bundleKey(li), parent: isBundleParent(li)
  }));

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

      // parent -> 0
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

  return { before, after };
}

// ========== memory ==========
const history = [];    // только MATCH
function remember(entry) { history.push(entry); while (history.length > 100) history.shift(); }
const last = () => (history.length ? history[history.length - 1] : null);

// ========== webhook ==========
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
    theme: mark.theme, debug: mark.debug,
    currency: order.currency, total_price: order.total_price,
    email: order.email,
    shipping_address: order.shipping_address,
    billing_address: order.billing_address,
    payload: conv,
    created_at: new Date().toISOString()
  });

  console.log("MATCH", order.id, order.name, mark, "items:", conv.after.length);
  res.status(200).send("ok");
});

// ========== debug & health ==========
app.get("/debug/last", (req,res)=>res.json(last()));
app.post("/test", express.text({ type: "*/*" }), (req,res)=>{ console.log("TEST HIT", new Date().toISOString(), req.headers["user-agent"]||""); res.send("ok"); });
app.get("/health", (req,res)=>res.send("ok"));

// ========== ShipStation (Custom Store XML) ==========
function basicAuth(req, res, next) {
  if (!process.env.SS_USER || !process.env.SS_PASS) return res.status(503).send("ShipStation auth not configured");
  const h = req.headers.authorization || "";
  if (!h.startsWith("Basic ")) return res.status(401).set("WWW-Authenticate","Basic").send("auth");
  const [u, p] = Buffer.from(h.slice(6), "base64").toString("utf8").split(":", 2);
  if (u === process.env.SS_USER && p === process.env.SS_PASS) return next();
  return res.status(401).set("WWW-Authenticate","Basic").send("auth");
}
function esc(x=""){ return String(x).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;"); }

function orderToXML(o){
  if (!o) return "<Orders total=\"0\" page=\"1\" pages=\"1\"></Orders>";
  const items = o.payload.after.filter(i=>!i.parent).map(i=>(
    `<Item>
      <SKU>${esc(i.sku||"")}</SKU>
      <Name>${esc(i.title)}</Name>
      <Quantity>${i.qty}</Quantity>
      <UnitPrice>${i.unitPrice.toFixed(2)}</UnitPrice>
    </Item>`
  )).join("");
  const shipTo = o.shipping_address||{};
  const billTo = o.billing_address||{};
  return `<?xml version="1.0" encoding="utf-8"?>
<Orders total="1" page="1" pages="1">
  <Order>
    <OrderID>${esc(String(o.id))}</OrderID>
    <OrderNumber>${esc(o.name)}</OrderNumber>
    <OrderDate>${new Date().toISOString()}</OrderDate>
    <OrderStatus>awaiting_shipment</OrderStatus>
    <BillTo>
      <Name>${esc([billTo.first_name,billTo.last_name].filter(Boolean).join(" "))}</Name>
      <Street1>${esc(billTo.address1||"")}</Street1>
      <City>${esc(billTo.city||"")}</City>
      <State>${esc(billTo.province_code||"")}</State>
      <PostalCode>${esc(billTo.zip||"")}</PostalCode>
      <Country>${esc(billTo.country_code||"")}</Country>
    </BillTo>
    <ShipTo>
      <Name>${esc([shipTo.first_name,shipTo.last_name].filter(Boolean).join(" "))}</Name>
      <Street1>${esc(shipTo.address1||"")}</Street1>
      <City>${esc(shipTo.city||"")}</City>
      <State>${esc(shipTo.province_code||"")}</State>
      <PostalCode>${esc(shipTo.zip||"")}</PostalCode>
      <Country>${esc(shipTo.country_code||"")}</Country>
    </ShipTo>
    <Items>${items}</Items>
  </Order>
</Orders>`;
}

// лог запросов от SS
function logSS(req, tag){
  console.log(`[SS] ${tag}`, req.method, req.originalUrl, req.headers["user-agent"]||"");
}

// единый обработчик для GET/POST/HEAD
function shipstationHandler(req, res) {
  logSS(req, "hit");
  res.type("text/xml"); // SS любит text/xml
  const method = req.method.toUpperCase();
  if (method === "HEAD") { res.status(200).end(); return; }

  const q = Object.fromEntries(Object.entries(req.query).map(([k,v])=>[k.toLowerCase(), String(v)]));
  const action = (q.action || "").toLowerCase();

  // тест соединения / статус
  if (action === "test" || action === "status") {
    res.send(`<?xml version="1.0" encoding="utf-8"?><Store><Status>OK</Status></Store>`);
    return;
  }

  // экспорт заказов (action=export) или без action
  let o = last();

  // фильтры по датам (не строго, просто демонстрация)
  // SS присылает ISO или yyyy-mm-dd hh:mm:ss
  const start = q.start_date ? Date.parse(q.start_date) : null;
  const end   = q.end_date   ? Date.parse(q.end_date)   : null;
  if (o && (start || end)) {
    const created = Date.parse(o.created_at || new Date().toISOString());
    if ((start && created < start) || (end && created > end)) {
      o = null;
    }
  }

  res.send(orderToXML(o));
}
app.get("/shipstation", basicAuth, shipstationHandler);
app.post("/shipstation", basicAuth, shipstationHandler);

// ========== start ==========
app.listen(process.env.PORT || 8080);
