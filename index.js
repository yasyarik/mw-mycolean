import express from "express";
import dotenv from "dotenv";
import getRawBody from "raw-body";
import crypto from "crypto";

dotenv.config();
const app = express();

console.log("MW UP", "secret_len", (process.env.SHOPIFY_WEBHOOK_SECRET || "").length);

// ---------------- utils ----------------
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

// ---- эвристики Simple Bundles / SKIO ----
function isBundleParent(li) {
  const p = li.properties || [];
  if (p.find(x => x.name === "_sb_parent" && String(x.value).toLowerCase() === "true")) return true;   // Simple Bundles
  if (p.find(x => x.name === "_bundle" || x.name === "bundle_id")) return true;                        // др. приложения
  if (p.find(x => x.name === "skio_parent" && String(x.value).toLowerCase() === "true")) return true;  // SKIO (если ставит)
  return false;
}
function bundleKey(li) {
  const p = li.properties || [];
  const f1 = p.find(x => ["_sb_bundle_id","bundle_id","_bundle_id","skio_bundle_id"].includes(x.name));
  if (f1) return String(f1.value);
  const f2 = p.find(x => ["_sb_key","bundle_key","skio_bundle_key"].includes(x.name));
  return f2 ? String(f2.value) : null;
}

// перерасчёт: родителю 0, цену раскладываем по детям пропорционально qty
function transformOrder(order) {
  const before = order.line_items.map(li => ({
    id: li.id, title: li.title, sku: li.sku || null, qty: li.quantity,
    price: toNum(li.price), total: toNum(li.price) * li.quantity,
    key: bundleKey(li), parent: isBundleParent(li)
  }));

  // группируем по bundle-key
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
      let qtySum = 0;
      for (const c of kids) qtySum += c.quantity;

      // родителя нулим
      after.push({ id: parent.id, title: parent.title, sku: parent.sku || null, qty: parent.quantity, unitPrice: 0, parent: true, key: k });

      // распределяем цену на детей
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
        // если у родителя нулевая цена — оставляем детей как есть
        for (const c of kids) {
          after.push({ id: c.id, title: c.title, sku: c.sku || null, qty: c.quantity, unitPrice: toNum(c.price), parent: false, key: k });
        }
      }
    }
  }

  // обычные позиции
  for (const li of order.line_items) {
    if (handled.has(li.id)) continue;
    after.push({ id: li.id, title: li.title, sku: li.sku || null, qty: li.quantity, unitPrice: toNum(li.price), parent: false, key: bundleKey(li) });
  }

  return { before, after };
}

// ---------------- storage ----------------
const history = []; // только MATCH (с меткой)
function remember(entry) { history.push(entry); while (history.length > 50) history.shift(); }
const last = () => (history.length ? history[history.length - 1] : null);

// ---------------- webhooks ----------------
app.post("/webhooks/orders-create", async (req, res) => {
  const raw = await getRawBody(req);
  const hdr = req.headers["x-shopify-hmac-sha256"] || "";
  const ok = hdr && hmacOk(raw, hdr, process.env.SHOPIFY_WEBHOOK_SECRET || "");
  if (!ok) { res.status(401).send("bad hmac"); return; }

  const order = JSON.parse(raw.toString("utf8"));
  const mark = pickMark(order);
  if (!mark || !String(mark.theme || "").startsWith("preview-")) { console.log("skip", order.id, order.name); res.status(200).send("skip"); return; }

  const conv = transformOrder(order);
  const entry = {
    id: order.id,
    name: order.name,
    theme: mark.theme,
    debug: mark.debug,
    currency: order.currency,
    total_price: order.total_price,
    email: order.email,
    shipping_address: order.shipping_address,
    billing_address: order.billing_address,
    payload: conv
  };
  remember(entry);
  console.log("MATCH", order.id, order.name, mark, "items:", conv.after.length);
  res.status(200).send("ok");
});

// ---------------- debug ----------------
app.get("/debug/last", (req, res) => { res.json(last()); });
app.post("/test", express.text({ type: "*/*" }), (req, res) => { console.log("TEST HIT", new Date().toISOString(), req.headers["user-agent"] || ""); res.send("ok"); });
app.get("/health", (req, res) => res.send("ok"));

// ---------------- ShipStation (read-only) ----------------
function basicAuth(req, res, next) {
  if (!process.env.SS_USER || !process.env.SS_PASS) return res.status(503).send("ShipStation auth not configured");
  const h = req.headers.authorization || "";
  const ok = h.startsWith("Basic ");
  if (!ok) return res.status(401).set("WWW-Authenticate","Basic").send("auth");
  const [u, p] = Buffer.from(h.slice(6), "base64").toString("utf8").split(":", 2);
  if (u === process.env.SS_USER && p === process.env.SS_PASS) return next();
  return res.status(401).set("WWW-Authenticate","Basic").send("auth");
}

function toShipStation(o) {
  if (!o) return null;
  const items = o.payload.after
    .filter(i => !i.parent) // склад видит только детей
    .map(i => ({ sku: i.sku || "", name: i.title, quantity: i.qty, unitPrice: i.unitPrice }));
  return {
    orders: [{
      orderId: String(o.id),
      orderNumber: o.name,
      orderDate: new Date().toISOString(),
      orderStatus: "awaiting_shipment",
      billTo: {
        name: o.billing_address ? [o.billing_address.first_name, o.billing_address.last_name].filter(Boolean).join(" ") : "",
        street1: o.billing_address?.address1 || "",
        city: o.billing_address?.city || "",
        state: o.billing_address?.province_code || "",
        postalCode: o.billing_address?.zip || "",
        country: o.billing_address?.country_code || ""
      },
      shipTo: {
        name: o.shipping_address ? [o.shipping_address.first_name, o.shipping_address.last_name].filter(Boolean).join(" ") : "",
        street1: o.shipping_address?.address1 || "",
        city: o.shipping_address?.city || "",
        state: o.shipping_address?.province_code || "",
        postalCode: o.shipping_address?.zip || "",
        country: o.shipping_address?.country_code || ""
      },
      items
    }],
    total: 1,
    page: 1,
    pages: 1
  };
}

app.get("/shipstation/ping", basicAuth, (req, res) => res.json({ ok: true, ts: new Date().toISOString() }));
app.get("/shipstation/orders", basicAuth, (req, res) => {
  // игнорируем фильтры для простоты — отдаём последний MATCH
  const o = last();
  res.json(toShipStation(o) || { orders: [], total: 0, page: 1, pages: 1 });
});

// ---------------- start ----------------
app.listen(process.env.PORT || 8080);
