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
  const n = Number(x);
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
    state: (d(a.province_code) || "ST").slice(0, 2).toUpperCase(),
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
  const month = pad(t.getUTCMonth() + 1);
  const day = pad(t.getUTCDate());
  const year = t.getUTCFullYear();
  const hours = pad(t.getUTCHours());
  const minutes = pad(t.getUTCMinutes());
  return `${month}/${day}/${year} ${hours}:${minutes}`;
}

function hasParentFlag(li){
  const p = Array.isArray(li?.properties) ? li.properties : [];
  const get = n => p.find(x => x.name === n)?.value;
  if (String(get("_sb_parent")).toLowerCase() === "true") return true;
  if (String(get("skio_parent")).toLowerCase() === "true") return true;
  if (get("_bundle") || get("bundle_id")) return true;
  return false;
}

function bundleKey(li){
  const p = Array.isArray(li?.properties) ? li.properties : [];
  const val = (n) => p.find(x => x.name === n)?.value;
  let v =
    val("_sb_bundle_id") ||
    val("bundle_id") ||
    val("_bundle_id") ||
    val("skio_bundle_id") ||
    val("_sb_key") ||
    val("bundle_key") ||
    val("skio_bundle_key");
  if (!v) {
    const g = val("_sb_bundle_group");
    if (g) v = String(g).split(" ")[0];
  }
  return v ? String(v) : null;
}

function anyAfterSellKey(li){
  const p = Array.isArray(li?.properties) ? li.properties : [];
  return p.some(x => {
    const n = String(x.name||"").toLowerCase();
    return n.includes("aftersell") || n.includes("after_sell") || n.includes("post_purchase");
  });
}

// === SIMPLE BUNDLES DETECT ===
function sbDetectFromOrder(order) {
  const json = JSON.stringify(order).toLowerCase();
if (json.includes("aftersell")) {
  return { children: order.line_items || [] };
}

  const items = Array.isArray(order.line_items) ? order.line_items : [];
  if (!items.length) return null;

  const tagStr = String(order.tags || "").toLowerCase();
  const epsilon = 0.00001;
  const zeroed = li => {
    const da = Array.isArray(li.discount_allocations)
      ? li.discount_allocations.reduce((s, d) => s + toNum(d.amount), 0)
      : 0;
    return (da >= toNum(li.price) - epsilon) || (toNum(li.total_discount) >= toNum(li.price) - epsilon);
  };

  const zeroChildren = items.filter(zeroed);
  const nonZero = items.filter(li => !zeroed(li));
  if (zeroChildren.length && nonZero.length >= 1) {
    return { children: zeroChildren };
  }
  if (zeroChildren.length && tagStr.includes("simple bundles")) {
    return { children: zeroChildren };
  }

  const groups = new Map();
  for (const li of items) {
    const k = bundleKey(li);
    if (!k && !hasParentFlag(li) && !anyAfterSellKey(li)) continue;
    const key = k || `__flag__${li.id}`;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(li);
  }

  const children = new Set();

  for (const arr of groups.values()) {
    if (arr.length < 2) continue;

    const zeros = arr.filter(zeroed);
    if (zeros.length) {
      zeros.forEach(li => children.add(li));
      continue;
    }

    if (arr.some(hasParentFlag)) {
      arr.forEach(li => { if (!hasParentFlag(li)) children.add(li); });
      continue;
    }
  }

  if (children.size > 0) {
    return { children: Array.from(children) };
  }

  return null;
}

function pushLine(after, li, imageUrl = "") {
  after.push({
    id: li.id,
    title: li.title,
    sku: li.sku || null,
    qty: li.quantity || 1,
    unitPrice: toNum(li.price),
    imageUrl
  });
}

const history = [];
const bestById = new Map();
const statusById = new Map();
const variantImageCache = new Map();

function remember(e) {
  history.push(e);
  while (history.length > 100) history.shift();
  bestById.set(String(e.id), e);
}

function getLastOrder() {
  return history.length > 0 ? history[history.length - 1] : [...bestById.values()][bestById.size - 1];
}
let SS_LAST_REFRESH_AT = 0;
let SS_REFRESH_TIMER = null;

function isMWOrder(order, conv){
  try {
    const items = Array.isArray(order?.line_items) ? order.line_items : [];
    const origCount = items.length;
    if (conv && Array.isArray(conv.after) && conv.after.length !== origCount) return true;

    const tags = String(order?.tags || "").toLowerCase();
    if (tags.includes("simple bundles")) return true;

    const epsilon = 0.00001;
    const zeroed = (li) => {
      const da = Array.isArray(li.discount_allocations)
        ? li.discount_allocations.reduce((s,d)=>s+toNum(d.amount),0)
        : 0;
      return (da >= toNum(li.price) - epsilon) || (toNum(li.total_discount) >= toNum(li.price) - epsilon);
    };

    for (const li of items) {
      if (li?.selling_plan_id || li?.selling_plan_allocation?.selling_plan_id) return true;

      const props = Array.isArray(li.properties) ? li.properties : [];
      if (props.some(p => {
        const n = String(p?.name || "").toLowerCase();
        return n.includes("_sb_") || n.includes("aftersell") || n.includes("after_sell") || n.includes("post_purchase");
      })) return true;

      if (zeroed(li)) return true;
    }
  } catch (_) {}
  return false;
}

async function ssRefreshNow(){
  const id = process.env.SS_STORE_ID;
  const key = process.env.SS_KEY;
  const sec = process.env.SS_SECRET;
  if (!id || !key || !sec) return;
  const auth = Buffer.from(`${key}:${sec}`).toString("base64");
  try {
    await fetch(`https://ssapi.shipstation.com/stores/refreshstore?storeId=${encodeURIComponent(id)}`, {
      method: "POST",
      headers: { Authorization: `Basic ${auth}` }
    });
  } catch (_) {}
}

function scheduleSSRefresh(){
  const now = Date.now();
  if (now - SS_LAST_REFRESH_AT < 20000) return;
  if (SS_REFRESH_TIMER) clearTimeout(SS_REFRESH_TIMER);
  SS_REFRESH_TIMER = setTimeout(async () => {
    SS_REFRESH_TIMER = null;
    await ssRefreshNow();
    SS_LAST_REFRESH_AT = Date.now();
  }, 5000);
}

async function getVariantImage(variantId) {
  if (!variantId) {
    console.log("getVariantImage: NO variant_id");
    return "";
  }
  const key = String(variantId);
  if (variantImageCache.has(key)) {
    const cached = variantImageCache.get(key);
    console.log(`CACHE HIT variant:${variantId} → ${cached || "EMPTY"}`);
    return cached;
  }

  const shop = process.env.SHOPIFY_SHOP;
  const token = process.env.SHOPIFY_ADMIN_ACCESS_TOKEN;

  if (!shop) {
    console.log("MISSING SHOPIFY_SHOP in .env");
    variantImageCache.set(key, "");
    return "";
  }
  if (!token) {
    console.log("MISSING SHOPIFY_ADMIN_ACCESS_TOKEN in .env");
    variantImageCache.set(key, "");
    return "";
  }

  const url = `https://${shop}/admin/api/2025-01/variants/${variantId}.json`;
  console.log(`FETCHING: ${url}`);

  try {
    const res = await fetch(url, {
      headers: { "X-Shopify-Access-Token": token }
    });

    console.log(`API RESPONSE: ${res.status} ${res.statusText}`);

    if (!res.ok) {
      const errText = await res.text();
      console.log(`API ERROR BODY: ${errText}`);
      throw new Error(`HTTP ${res.status}`);
    }

    const data = await res.json();
    const variant = data.variant;
    if (!variant) {
      console.log("NO variant in response");
      variantImageCache.set(key, "");
      return "";
    }

    let imageUrl = variant.image?.src || "";
    console.log(`VARIANT IMAGE: ${imageUrl || "NONE"}`);

    if (!imageUrl && variant.product_id) {
      console.log(`FALLBACK: fetching product ${variant.product_id}`);
      const prodRes = await fetch(
        `https://${shop}/admin/api/2025-01/products/${variant.product_id}.json?fields=images`,
        { headers: { "X-Shopify-Access-Token": token } }
      );
      if (prodRes.ok) {
        const p = await prodRes.json();
        imageUrl = p.product.images?.[0]?.src || "";
        console.log(`PRODUCT IMAGE: ${imageUrl || "NONE"}`);
      }
    }

    variantImageCache.set(key, imageUrl);
    return imageUrl;
  } catch (e) {
    console.error(`IMAGE FETCH FAILED (variant ${variantId}):`, e.message);
    variantImageCache.set(key, "");
    return "";
  }
}

async function transformOrder(order) {
  const after = [];
  const handled = new Set();
  const tags = String(order.tags || "").toLowerCase();
  const anyBundle = sbDetectFromOrder(order);
  const hasBundleTag = tags.includes("simple bundles") || tags.includes("bundle") || tags.includes("skio") || tags.includes("aftersell");

  if (!anyBundle && !hasBundleTag) {
    console.log("SKIP ORDER (no bundle detected):", order.id, order.name);
    return { after: [] };
  }

  const sb = sbDetectFromOrder(order);
  if (sb) {
    for (const c of sb.children) {
      handled.add(c.id);
      const imageUrl = await getVariantImage(c.variant_id);
      pushLine(after, c, imageUrl);
      console.log("SB CHILD", { id: c.id, title: c.title, variant_id: c.variant_id, imageUrl: imageUrl ? "OK" : "NO" });
    }
    return { after };
  }

  for (const li of (order.line_items || [])) {
    if (handled.has(li.id)) continue;
    const imageUrl = await getVariantImage(li.variant_id);
    after.push({
      id: li.id,
      title: li.title,
      sku: li.sku || null,
      qty: li.quantity || 1,
      unitPrice: toNum(li.price),
      imageUrl
    });
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
    remember({
      id: order.id,
      name: order.name,
      email: order.email || "",
      shipping_address: order.shipping_address || {},
      billing_address: order.billing_address || {},
      payload: conv,
      created_at: order.created_at || new Date().toISOString()
    });
    statusById.set(order.id, "awaiting_shipment");
    if (isMWOrder(order, conv)) {
  scheduleSSRefresh();
}

    console.log("ORDER PROCESSED", order.id, "items:", conv.after.length);
    await refreshStoreNow();

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
  if (!o || !o.payload?.after?.length) {
    return `<?xml version="1.0" encoding="utf-8"?><Orders></Orders>`;
  }

  const items = o.payload.after.map(i => ({
    id: i.id,
    title: i.title,
    sku: i.sku || "",
    qty: i.qty,
    unitPrice: i.unitPrice,
    imageUrl: i.imageUrl || ""
  }));

  const total = items.reduce((s, i) => s + i.unitPrice * i.qty, 0);
  const bill = filledAddress(o.billing_address || {});
  const ship = filledAddress(o.shipping_address || o.billing_address || {});
  const email = (o.email && o.email.includes("@")) ? o.email : "customer@example.com";
  const orderDate = fmtShipDate(new Date(o.created_at || Date.now()));
  const lastMod = fmtShipDate(new Date());
  const shipStationStatus = "awaiting_shipment";

  const itemsXml = items.map(i => `
    <Item>
      <LineItemID>${esc(i.id)}</LineItemID>
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
    <OrderID>${esc(o.id)}</OrderID>
    <OrderNumber>${esc(o.name.replace(/^#/, ''))}</OrderNumber>
    <OrderDate>${orderDate}</OrderDate>
    <OrderStatus>${shipStationStatus}</OrderStatus>
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
    order = bestById.get(String(id));
  } else {
    order = getLastOrder();
  }
  res.send(minimalXML(order || { payload: { after: [] } }));
});

app.get("/health", (req, res) => res.send("OK"));

const PORT = process.env.PORT || 8080;
async function refreshStoreNow() {
  const storeId = process.env.SS_STORE_ID;
  const key = process.env.SS_V2_KEY;
  const secret = process.env.SS_V2_SECRET || "";
  if (!storeId || !key) {
    console.log("Missing ShipStation API credentials");
    return;
  }

  const auth = Buffer.from(`${key}:${secret}`).toString("base64");
  const res = await fetch(
    `https://ssapi.shipstation.com/stores/refreshstore?storeId=${storeId}`,
    {
      method: "POST",
      headers: { Authorization: `Basic ${auth}` },
    }
  );

  console.log("ShipStation refreshstore →", res.status, await res.text());
}

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
