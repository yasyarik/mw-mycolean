import express from "express";
import dotenv from "dotenv";
import getRawBody from "raw-body";
import crypto from "crypto";

dotenv.config();
const app = express();

function hmacOk(raw, header, secret) {
  if (!header || !secret) return false;
  const sig = crypto.createHmac("sha256", secret).update(raw).digest("base64");
  const a = Buffer.from(header), b = Buffer.from(sig);
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

function toNum(x) {
  const n = Number.parseFloat(String(x ?? "0"));
  return Number.isFinite(n) ? n : 0;
}

function esc(x = "") {
  return String(x).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

function money2(v) {
  return (Number.isFinite(v) ? v : 0).toFixed(2);
}

function filledAddress(a) {
  const d = v => (v && String(v).trim()) ? String(v) : "";
  const name = [d(a.first_name), d(a.last_name)].filter(Boolean).join(" ") || "Customer";
  return {
    name,
    company: d(a.company) || "",
    phone: d(a.phone) || "",
    email: d(a.email) || "",
    address1: d(a.address1) || "Address line 1",
    address2: d(a.address2) || "",
    city: d(a.city) || "City",
    state: (d(a.province_code) || d(a.province) || "ST").slice(0, 2).toUpperCase(),
    zip: d(a.zip) || "00000",
    country: (d(a.country_code) || "US").slice(0, 2).toUpperCase()
  };
}

function skuSafe(i, orderId) {
  const s = (i.sku || "").trim();
  if (s) return s;
  return `MW-${orderId}-${i.id || "ITEM"}`;
}

function fmtShipDate(d = new Date()) {
  const t = new Date(d);
  const pad = n => String(n).padStart(2, "0");
  return `${pad(t.getUTCMonth() + 1)}/${pad(t.getUTCDate())}/${t.getUTCFullYear()} ${pad(t.getUTCHours())}:${pad(t.getUTCMinutes())}`;
}

function isBundleParent(li) {
  const p = li.properties || [];
  if (p.find(x => x.name === "_sb_parent" && String(x.value).toLowerCase() === "true")) return true;
  if (p.find(x => x.name === "_bundle" || x.name === "bundle_id")) return true;
  if (p.find(x => x.name === "skio_parent" && String(x.value).toLowerCase() === "true")) return true;
  return false;
}

function bundleKey(li) {
  const p = li.properties || [];
  const get = n => {
    const f = p.find(x => x.name === n);
    return f ? String(f.value) : null;
  };
  let v =
    get("_sb_bundle_id") ||
    get("bundle_id") ||
    get("_bundle_id") ||
    get("skio_bundle_id") ||
    get("_sb_key") ||
    get("bundle_key") ||
    get("skio_bundle_key");
  if (!v) {
    const g = get("_sb_bundle_group");
    if (g) v = String(g).split(" ")[0];
  }
  return v;
}

function parseSbComponents(li) {
  const props = Array.isArray(li?.properties) ? li.properties : [];
  const rawVals = props
    .filter(p => ["_sb_bundle_variant_id_qty", "_sb_components", "_sb_child_id_qty"].includes(p.name))
    .map(p => String(p.value))
    .filter(Boolean);
  const out = [];
  for (const v of rawVals) {
    const s = v.trim();
    if ((s.startsWith("[") && s.endsWith("]")) || (s.startsWith("{") && s.endsWith("}"))) {
      try {
        const parsed = JSON.parse(s);
        const arr = Array.isArray(parsed) ? parsed : [parsed];
        for (const it of arr) {
          const vid = String(it.variant_id || it.variantId || "").replace(/^gid:\/\/shopify\/ProductVariant\//, "");
          const qty = Number(it.qty || it.quantity || it.qty_each || it.count || 0);
          if (vid && qty > 0) out.push({ variantId: vid, qty });
        }
      } catch (_) {}
    }
  }
  const flat = rawVals.join("|");
  if (flat) {
    const parts = flat.split(/[,;|]/).map(x => x.trim()).filter(Boolean);
    for (const p of parts) {
      const m = p.match(/^(?:gid:\/\/shopify\/ProductVariant\/)?(\d+)\s*:\s*(\d+)$/i);
      if (m) out.push({ variantId: m[1], qty: Number(m[2]) || 0 });
    }
  }
  const dedup = new Map();
  for (const r of out) {
    if (!r.variantId || r.qty <= 0) continue;
    const k = r.variantId;
    dedup.set(k, (dedup.get(k) || 0) + r.qty);
  }
  return [...dedup.entries()].map(([variantId, qty]) => ({ variantId, qty }));
}

function sbDetectFromOrder(order) {
  const items = Array.isArray(order.line_items) ? order.line_items : [];
  if (!items.length) return null;
  const epsilon = 0.00001;
  const daSum = li => (Array.isArray(li.discount_allocations) ? li.discount_allocations.reduce((s, d) => s + toNum(d.amount), 0) : 0);
  const zeroed = li => (daSum(li) >= toNum(li.price) - epsilon) || (toNum(li.total_discount) >= toNum(li.price) - epsilon);
  const children = items.filter(li => zeroed(li));
  const parents = items.filter(li => !zeroed(li));
  if (children.length && parents.length === 1) {
    return { parent: parents[0], children };
  }
  const tagStr = String(order.tags || "").toLowerCase();
  const withPrice = items.filter(li => toNum(li.price) > 0);
  const maxPrice = withPrice.length ? Math.max(...withPrice.map(li => toNum(li.price))) : 0;
  const parentGuess = withPrice.find(li => toNum(li.price) === maxPrice) || null;
  if (tagStr.includes("simple bundles") && parentGuess && items.length > 1) {
    const rest = items.filter(li => li !== parentGuess);
    return { parent: parentGuess, children: rest };
  }
  return null;
}

function pushLine(after, li, { unitPrice = null, imageUrl = "" } = {}) {
  const price = unitPrice != null ? unitPrice : toNum(li.price);
  after.push({
    id: li.id,
    title: li.title,
    sku: li.sku || null,
    qty: li.quantity || 1,
    unitPrice: price,
    imageUrl
  });
}

const history = [];
const statusById = new Map();
const variantImageCache = new Map();
const variantPriceCache = new Map();
const bestById = new Map();

function childrenCount(o) {
  return Array.isArray(o?.payload?.after) ? o.payload.after.length : 0;
}

function remember(e) {
  history.push(e);
  while (history.length > 100) history.shift();
  const key = String(e.id);
  const prev = bestById.get(key);
  if (!prev || childrenCount(e) >= childrenCount(prev)) {
    bestById.set(key, e);
  }
}

function getLastOrder() {
  if (history.length > 0) return history[history.length - 1];
  const arr = [...bestById.values()];
  return arr[arr.length - 1];
}

async function getVariantImage(variantId) {
  if (!variantId) return "";
  const key = String(variantId);
  if (variantImageCache.has(key)) return variantImageCache.get(key);
  const shop = process.env.SHOPIFY_SHOP;
  const token = process.env.SHOPIFY_ADMIN_ACCESS_TOKEN;
  if (!shop || !token) { variantImageCache.set(key, ""); return ""; }
  try {
    const url = `https://${shop}/admin/api/2025-01/variants/${variantId}.json`;
    const res = await fetch(url, { headers: { "X-Shopify-Access-Token": token } });
    if (res.ok) {
      const data = await res.json();
      const v = data?.variant;
      let imageUrl = v?.image?.src || "";
      if (!imageUrl && v?.product_id) {
        const pr = await fetch(`https://${shop}/admin/api/2025-01/products/${v.product_id}.json?fields=images`, { headers: { "X-Shopify-Access-Token": token } });
        if (pr.ok) {
          const pj = await pr.json();
          imageUrl = pj?.product?.images?.[0]?.src || "";
        }
      }
      variantImageCache.set(key, imageUrl || "");
      return imageUrl || "";
    }
  } catch (_) {}
  variantImageCache.set(key, "");
  return "";
}

async function fetchVariantPrice(variantId) {
  if (!variantId) return 0;
  const key = String(variantId);
  if (variantPriceCache.has(key)) return variantPriceCache.get(key);
  const shop = process.env.SHOPIFY_SHOP;
  const token = process.env.SHOPIFY_ADMIN_ACCESS_TOKEN;
  if (!shop || !token) { variantPriceCache.set(key, 0); return 0; }
  try {
    const r = await fetch(`https://${shop}/admin/api/2025-01/variants/${key}.json`, { headers: { "X-Shopify-Access-Token": token } });
    if (!r.ok) { variantPriceCache.set(key, 0); return 0; }
    const j = await r.json();
    const price = toNum(j?.variant?.price);
    variantPriceCache.set(key, price);
    return price;
  } catch (_) { variantPriceCache.set(key, 0); return 0; }
}

async function fetchBundleRecipe(productId, variantId) {
  const shop = process.env.SHOPIFY_SHOP;
  const token = process.env.SHOPIFY_ADMIN_ACCESS_TOKEN;
  if (!shop || !token || !productId) return null;
  const tryKeys = [
    { ns: "simple_bundles_2_0", key: "components" },
    { ns: "simple_bundles", key: "components" },
    { ns: "simplebundles", key: "components" },
    { ns: "bundles", key: "components" }
  ];
  const gql = `
  query($pid: ID!, $vKeys: [HasMetafieldsIdentifier!]!) {
    product(id: $pid) {
      metafields(identifiers: $vKeys){ namespace key value type }
    }
  }`;
  try {
    const r = await fetch(`https://${shop}/admin/api/2025-01/graphql.json`, {
      method: "POST",
      headers: { "X-Shopify-Access-Token": token, "Content-Type": "application/json" },
      body: JSON.stringify({ query: gql, variables: { pid: `gid://shopify/Product/${productId}`, vKeys: tryKeys } })
    });
    if (!r.ok) return null;
    const j = await r.json();
    const mfs = j?.data?.product?.metafields || [];
    const mf = mfs.find(m => m?.value);
    if (!mf) return null;
    let parsed = null;
    try { parsed = JSON.parse(mf.value); } catch (_) {}
    if (!parsed) return null;
    const list = Array.isArray(parsed) ? parsed : (Array.isArray(parsed.components) ? parsed.components : []);
    const out = [];
    for (const it of list) {
      const vid = String(it.variantId || it.variant_id || "").replace(/^gid:\/\/shopify\/ProductVariant\//, "");
      const qty = Number(it.quantity || it.qty || it.qty_each || 0);
      if (vid && qty > 0) out.push({ variantId: vid, qty });
    }
    return out.length ? out : null;
  } catch (_) { return null; }
}

async function transformOrder(order) {
  const after = [];
  const handled = new Set();

  const groups = {};
  for (const li of (order.line_items || [])) {
    const k = bundleKey(li);
    if (!k) continue;
    if (!groups[k]) groups[k] = { parent: null, children: [] };
    if (isBundleParent(li)) groups[k].parent = li; else groups[k].children.push(li);
  }

  for (const [k, g] of Object.entries(groups)) {
    const parent = g.parent, kids = g.children;
    if (parent) handled.add(parent.id);
    for (const c of kids) handled.add(c.id);
    if (parent && kids.length > 0) {
      const totalParent = toNum(parent.price) * (parent.quantity || 1);
      const pricedKids = [];
      const zeroKids = [];
      for (const c of kids) {
        const p = toNum(c.price);
        if (p > 0) pricedKids.push({ c, p }); else zeroKids.push(c);
      }
      if (zeroKids.length) {
        for (const zk of zeroKids) {
          const vid = zk.variant_id || zk.variantId || null;
          const vp = vid ? await fetchVariantPrice(vid) : 0;
          pricedKids.push({ c: zk, p: toNum(vp) });
        }
      }
      if (pricedKids.some(x => x.p > 0)) {
        for (const { c, p } of pricedKids) {
          const img = await getVariantImage(c.variant_id);
          pushLine(after, c, { unitPrice: toNum(p), imageUrl: img });
        }
      } else {
        let qtySum = kids.reduce((s, c) => s + (c.quantity || 0), 0);
        qtySum = Math.max(1, qtySum);
        let rest = Math.round(totalParent * 100);
        for (let i = 0; i < kids.length; i++) {
          const c = kids[i];
          const share = i === kids.length - 1 ? rest : Math.round((totalParent * ((c.quantity || 0) / qtySum)) * 100);
          rest -= share;
          const unit = (c.quantity || 1) > 0 ? share / (c.quantity || 1) / 100 : 0;
          const img = await getVariantImage(c.variant_id);
          pushLine(after, c, { unitPrice: Number(unit.toFixed(2)), imageUrl: img });
        }
      }
    }
  }

  if (after.length === 0) {
    const sb = sbDetectFromOrder(order);
    if (sb) {
      const { parent, children } = sb;
      if (parent) handled.add(parent.id);
      for (const c of children) {
        handled.add(c.id);
        const img = await getVariantImage(c.variant_id);
        pushLine(after, c, { imageUrl: img });
      }
    }
  }

  if (after.length === 0) {
    for (const li of (order.line_items || [])) {
      if (handled.has(li.id)) continue;
      const comps = parseSbComponents(li);
      if (!comps.length) continue;
      const priced = [];
      let anyPrice = false;
      for (const c of comps) {
        const vp = await fetchVariantPrice(c.variantId);
        const price = toNum(vp);
        if (price > 0) anyPrice = true;
        priced.push({ variantId: c.variantId, qty: c.qty, price });
      }
      if (anyPrice) {
        for (const x of priced) {
          const img = await getVariantImage(x.variantId);
          after.push({ id: `${li.id}::${x.variantId}`, title: li.title, sku: li.sku || null, qty: x.qty, unitPrice: toNum(x.price), imageUrl: img });
        }
      } else {
        const total = toNum(li.price) * (li.quantity || 1);
        const qtySum = Math.max(1, comps.reduce((s, c) => s + (c.qty || 0), 0));
        let rest = Math.round(total * 100);
        for (let i = 0; i < comps.length; i++) {
          const c = comps[i];
          const share = i === comps.length - 1 ? rest : Math.round((total * ((c.qty || 0) / qtySum)) * 100);
          rest -= share;
          const unit = (c.qty || 1) > 0 ? share / (c.qty || 1) / 100 : 0;
          const img = await getVariantImage(c.variantId);
          after.push({ id: `${li.id}::${c.variantId}`, title: li.title, sku: li.sku || null, qty: c.qty || 1, unitPrice: Number(unit.toFixed(2)), imageUrl: img });
        }
      }
      handled.add(li.id);
    }
  }

  if (after.length === 0) {
    const items = order.line_items || [];
    if (items.length === 1) {
      const li = items[0];
      const comps = await fetchBundleRecipe(li.product_id, li.variant_id);
      if (Array.isArray(comps) && comps.length) {
        for (const c of comps) {
          const price = await fetchVariantPrice(c.variantId);
          const img = await getVariantImage(c.variantId);
          after.push({ id: `${li.id}::${c.variantId}`, title: li.title, sku: li.sku || null, qty: (c.qty || 1) * (li.quantity || 1), unitPrice: toNum(price), imageUrl: img });
        }
        handled.add(li.id);
      }
    }
  }

  for (const li of (order.line_items || [])) {
    if (handled.has(li.id)) continue;
    const img = await getVariantImage(li.variant_id);
    pushLine(after, li, { imageUrl: img });
  }

  if (after.length === 0) {
    for (const li of (order.line_items || [])) {
      const img = await getVariantImage(li.variant_id);
      pushLine(after, li, { imageUrl: img });
    }
  }

  return { after };
}

async function handleOrderCreateOrUpdate(req, res) {
  try {
    const raw = await getRawBody(req);
    const hdr = req.headers["x-shopify-hmac-sha256"] || "";
    if (!hmacOk(raw, hdr, process.env.SHOPIFY_WEBHOOK_SECRET || "")) {
      res.status(401).send("bad hmac");
      return;
    }
    const order = JSON.parse(raw.toString("utf8"));
    const conv = await transformOrder(order);
    const entry = {
      id: order.id,
      name: order.name,
      email: order.email || "",
      shipping_address: order.shipping_address || {},
      billing_address: order.billing_address || {},
      payload: conv,
      created_at: order.created_at || new Date().toISOString()
    };
    remember(entry);
    statusById.set(order.id, "awaiting_shipment");
    console.log("ORDER PROCESSED", order.id, "items:", conv.after.length);
    res.status(200).send("ok");
  } catch (e) {
    console.error("Webhook error:", e);
    res.status(500).send("error");
  }
}

app.post("/webhooks/orders-create", handleOrderCreateOrUpdate);
app.post("/webhooks/orders-updated", handleOrderCreateOrUpdate);

app.post("/webhooks/orders-cancelled", async (req, res) => {
  try {
    const raw = await getRawBody(req);
    const hdr = req.headers["x-shopify-hmac-sha256"] || "";
    if (!hmacOk(raw, hdr, process.env.SHOPIFY_WEBHOOK_SECRET || "")) {
      res.status(401).send("bad hmac");
      return;
    }
    const order = JSON.parse(raw.toString("utf8"));
    statusById.set(order.id, "cancelled");
    console.log("ORDER CANCELLED", order.id);
    res.status(200).send("ok");
  } catch (e) {
    console.error("Cancel webhook error:", e);
    res.status(500).send("error");
  }
});

function minimalXML(o) {
  if (!o || !o.payload?.after?.length) return `<?xml version="1.0" encoding="utf-8"?><Orders></Orders>`;
  const items = o.payload.after.map(i => ({
    id: i.id,
    title: i.title,
    sku: i.sku || "",
    qty: Math.max(1, parseInt(i.qty || 1, 10)),
    unitPrice: Number.isFinite(i.unitPrice) ? i.unitPrice : 0,
    imageUrl: i.imageUrl || ""
  }));
  const total = items.reduce((s, i) => s + i.unitPrice * i.qty, 0);
  const bill = filledAddress(o.billing_address || {});
  const ship = filledAddress(o.shipping_address || o.billing_address || {});
  const email = (o.email && o.email.includes("@")) ? o.email : "customer@example.com";
  const orderDate = fmtShipDate(new Date(o.created_at || Date.now()));
  const lastMod = fmtShipDate(new Date());
  const status = statusById.get(o.id) || "awaiting_shipment";
  const itemsXml = items.map(i => `
    <Item>
      <LineItemID>${esc(String(i.id))}</LineItemID>
      <SKU>${esc(skuSafe(i, o.id))}</SKU>
      <Name>${esc(i.title)}</Name>
      <Quantity>${i.qty}</Quantity>
      <UnitPrice>${money2(i.unitPrice)}</UnitPrice>
      <ImageUrl>${esc(i.imageUrl)}</ImageUrl>
      <Adjustment>false</Adjustment>
    </Item>`).join("");
  return `<?xml version="1.0" encoding="utf-8"?>
<Orders>
  <Order>
    <OrderID>${esc(String(o.id))}</OrderID>
    <OrderNumber>${esc(o.name || String(o.id))}</OrderNumber>
    <OrderDate>${orderDate}</OrderDate>
    <OrderStatus>${status}</OrderStatus>
    <LastModified>${lastMod}</LastModified>
    <ShippingMethod>Ground</ShippingMethod>
    <PaymentMethod>Other</PaymentMethod>
    <CurrencyCode>USD</CurrencyCode>
    <OrderTotal>${money2(total)}</OrderTotal>
    <TaxAmount>0.00</TaxAmount>
    <ShippingAmount>0.00</ShippingAmount>
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
    <Items>${itemsXml}</Items>
  </Order>
</Orders>`;
}

function authOK(req) {
  const h = req.headers.authorization || "";
  if (h.startsWith("Basic ")) {
    const [u, p] = Buffer.from(h.slice(6), "base64").toString("utf8").split(":", 2);
    return u === process.env.SS_USER && p === process.env.SS_PASS;
  }
  return false;
}

app.use("/shipstation", (req, res) => {
  res.set("Content-Type", "application/xml; charset=utf-8");
  if (!process.env.SS_USER || !process.env.SS_PASS || !authOK(req)) {
    res.status(401).send(`<?xml version="1.0" encoding="utf-8"?><Error>Authentication failed</Error>`);
    return;
  }
  const id = req.query.order_id;
  let order;
  if (id) {
    const wanted = String(id);
    order = bestById.get(wanted) || history.slice().reverse().find(o => String(o.id) === wanted);
  } else {
    order = getLastOrder();
  }
  res.send(minimalXML(order || { payload: { after: [] } }));
});

app.get("/health", (req, res) => res.send("OK"));

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
