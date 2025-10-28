import express from "express";
import dotenv from "dotenv";
import getRawBody from "raw-body";
import crypto from "crypto";

dotenv.config();
const app = express();

function hmacOk(raw, header, secret) {
  if (!header) return false;
  const sig = crypto.createHmac("sha256", secret).update(raw).digest("base64");
  const a = Buffer.from(header), b = Buffer.from(sig);
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}
function toNum(x){ const n = Number.parseFloat(String(x||"0")); return Number.isFinite(n)?n:0; }
function fmtShipDate(d=new Date()){
  const t=new Date(d); const pad=n=>String(n).padStart(2,"0");
  return `${pad(t.getUTCMonth()+1)}/${pad(t.getUTCDate())}/${t.getUTCFullYear()} ${pad(t.getUTCHours())}:${pad(t.getUTCMinutes())}`;
}
function esc(x=""){ return String(x).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;"); }
function money2(v){ return (Number.isFinite(v)?v:0).toFixed(2); }
function normState(s){ const v=(s||"").trim(); if(!v) return "ST"; return v.length===2?v:v.slice(0,2).toUpperCase(); }
function filledAddress(a){
  const d=v=>(v&&String(v).trim())?String(v):"";
  const name=[d(a.first_name),d(a.last_name)].filter(Boolean).join(" ")||"Customer";
  return {
    name, company:d(a.company)||"", phone:d(a.phone)||"", email:d(a.email)||"",
    address1:d(a.address1)||"Address line 1", address2:d(a.address2)||"",
    city:d(a.city)||"City", state:normState(a.province_code||a.province),
    zip:d(a.zip)||"00000", country:(d(a.country_code)||"US").slice(0,2).toUpperCase()
  };
}
function skuSafe(i, orderId){
  const s=(i.sku||"").trim(); if(s) return s;
  const base=i.id?String(i.id):(i.title?i.title.replace(/\s+/g,"-").slice(0,24):"ITEM");
  return `MW-${orderId}-${base}`;
}

function getVariantIdFromProperties(li) {
  const props = li.properties || [];
  const keys = ["_sb_variant_id", "variant_id", "_bundle_variant_id", "component_variant_id"];
  for (const key of keys) {
    const prop = props.find(p => p.name === key);
    if (prop && prop.value) {
      const match = String(prop.value).match(/(\d+)$/);
      if (match) return match[1];
    }
  }
  return null;
}

/* ПРОСТО: тянем URL прямо из свойств, если бандлер положил */
function extractImageUrlFromProps(li){
  const props = Array.isArray(li?.properties) ? li.properties : [];
  const urlLike = v => typeof v === "string" && /^https?:\/\//i.test(v);
  const goodName = n => /image|img|thumbnail|thumb|_sb_image/i.test(String(n||""));
  for (const p of props) {
    if (!p) continue;
    const v = p.value;
    if (urlLike(v) && goodName(p.name)) return String(v).trim();
  }
  for (const p of props) {
    const v = String(p?.value||"").trim();
    if (urlLike(v) && /cdn\.shopify\.com|\.jpg|\.jpeg|\.png|\.webp/i.test(v)) return v;
  }
  return "";
}

function sbDetectFromOrder(order){
  const items = Array.isArray(order.line_items) ? order.line_items : [];
  const tagStr = String(order.tags || "").toLowerCase();
  if (!items.length) return null;
  const epsilon = 0.00001;
  const daSum = li => (Array.isArray(li.discount_allocations) ? li.discount_allocations.reduce((s,d)=>s+toNum(d.amount),0) : 0);
  const zeroed = li => (daSum(li) >= toNum(li.price) - epsilon) || (toNum(li.total_discount) >= toNum(li.price) - epsilon);
  const children = items.filter(li => zeroed(li));
  const parents = items.filter(li => !zeroed(li));
  if (children.length && parents.length === 1) {
    return { parent: parents[0], children };
  }
  if (!children.length && tagStr.includes("simple bundles")) {
    return { parent: null, children: items };
  }
  return null;
}

function pushLine(after, li, { unitPrice = null, key = null, imageUrl = null } = {}){
  const price = unitPrice != null ? unitPrice : toNum(li.price);
  after.push({
    id: li.id,
    title: li.title,
    sku: li.sku || null,
    qty: li.quantity || 1,
    unitPrice: price,
    key,
    imageUrl
  });
}

const history = [];
const last = () => history.length ? history[history.length-1] : null;
const statusById = new Map();
const variantDetailsCache = new Map();
const productImagesCache = new Map();
const bestById = new Map();

function remember(e){
  history.push(e);
  while(history.length>100) history.shift();
  const key = String(e.id);
  const prev = bestById.get(key);
  if (!prev || e.payload.after.length >= prev.payload.after.length) {
    bestById.set(key, e);
  }
}

/* КЭШИРОВАННЫЕ простые REST-вызовы для картинок */
async function fetchProductFirstImageUrl(shop, token, productId){
  const pk = String(productId);
  if (productImagesCache.has(pk)) return productImagesCache.get(pk);
  const res = await fetch(`https://${shop}/admin/api/2025-01/products/${productId}.json?fields=images`, { headers: { "X-Shopify-Access-Token": token } });
  if (!res.ok) { productImagesCache.set(pk, ""); return ""; }
  const j = await res.json();
  const url = j?.product?.images?.[0]?.src || "";
  productImagesCache.set(pk, url || "");
  return url || "";
}
async function fetchProductImageById(shop, token, productId, imageId){
  const key = `${productId}:${imageId}`;
  if (productImagesCache.has(key)) return productImagesCache.get(key);
  const res = await fetch(`https://${shop}/admin/api/2025-01/products/${productId}/images/${imageId}.json`, { headers: { "X-Shopify-Access-Token": token } });
  if (!res.ok) { productImagesCache.set(key, ""); return ""; }
  const j = await res.json();
  const url = j?.image?.src || "";
  productImagesCache.set(key, url || "");
  return url || "";
}
async function fetchProductImageForVariant(shop, token, productId, variantId){
  const key = `map:${productId}`;
  if (productImagesCache.has(key)) {
    const map = productImagesCache.get(key);
    return map.get(String(variantId)) || map.get("first") || "";
  }
  const res = await fetch(`https://${shop}/admin/api/2025-01/products/${productId}.json?fields=images`, {
    headers: { "X-Shopify-Access-Token": token }
  });
  const map = new Map();
  if (res.ok) {
    const j = await res.json();
    const images = Array.isArray(j?.product?.images) ? j.product.images : [];
    if (images.length) map.set("first", images[0]?.src || "");
    for (const img of images) {
      const ids = Array.isArray(img?.variant_ids) ? img.variant_ids : [];
      for (const vid of ids) map.set(String(vid), img?.src || "");
    }
  }
  productImagesCache.set(key, map);
  return map.get(String(variantId)) || map.get("first") || "";
}

/* УПРОЩЁННЫЙ универсальный резолвер картинки для линии */
async function bestImageUrlForLineItem(li){
  const fromProps = extractImageUrlFromProps(li);
  if (fromProps) return fromProps;
  const shop = process.env.SHOPIFY_SHOP;
  const token = process.env.SHOPIFY_ADMIN_ACCESS_TOKEN;
  if (!shop || !token) return "";
  const vid = li.variant_id || getVariantIdFromProperties(li) || null;
  const pid = li.product_id || null;
  if (pid && li.image?.src) return String(li.image.src);
  if (vid) {
    const vRes = await fetch(`https://${shop}/admin/api/2025-01/variants/${vid}.json?fields=product_id,image_id`, { headers: { "X-Shopify-Access-Token": token } });
    if (vRes.ok) {
      const vj = await vRes.json();
      const productId = vj?.variant?.product_id || pid;
      const imageId = vj?.variant?.image_id || null;
      if (productId && imageId) {
        const u = await fetchProductImageById(shop, token, productId, imageId);
        if (u) return u;
      }
      if (productId) {
        const u2 = await fetchProductImageForVariant(shop, token, productId, vid);
        if (u2) return u2;
        const u3 = await fetchProductFirstImageUrl(shop, token, productId);
        if (u3) return u3;
      }
    }
  }
  if (pid) {
    const u = await fetchProductFirstImageUrl(shop, token, pid);
    if (u) return u;
  }
  return "";
}

async function transformOrder(order){
  const after = [];
  const handled = new Set();
  const sb = sbDetectFromOrder(order);
  if (sb) {
    const { children } = sb;
    for (const c of children) {
      handled.add(c.id);
      const imageUrl = await bestImageUrlForLineItem(c);
      pushLine(after, c, { imageUrl });
    }
    return { after };
  }
  for(const li of (order.line_items||[])){
    if(handled.has(li.id)) continue;
    const imageUrl = await bestImageUrlForLineItem(li);
    after.push({
      id: li.id,
      title: li.title,
      sku: li.sku || null,
      qty: li.quantity || 1,
      unitPrice: toNum(li.price),
      key: null,
      imageUrl
    });
  }
  return { after };
}

app.post("/webhooks/orders-create", async (req,res)=>{
  try{
    const raw=await getRawBody(req);
    const hdr=req.headers["x-shopify-hmac-sha256"]||"";
    if(!hmacOk(raw,hdr,process.env.SHOPIFY_WEBHOOK_SECRET||"")){ res.status(401).send("bad hmac"); return; }
    const order=JSON.parse(raw.toString("utf8"));
    const conv=await transformOrder(order);
    remember({
      id:order.id, name:order.name, currency:order.currency, total_price:toNum(order.total_price),
      email:order.email||"", shipping_address:order.shipping_address||{}, billing_address:order.billing_address||{},
      payload:conv, created_at:order.created_at||new Date().toISOString()
    });
    statusById.set(order.id,"awaiting_shipment");
    console.log("MATCH",order.id,order.name,"items:",conv.after.length);
    res.status(200).send("ok");
  }catch(e){ console.error(e); res.status(500).send("err"); }
});

app.post("/webhooks/orders-updated", async (req,res)=>{
  try{
    const raw=await getRawBody(req);
    const hdr=req.headers["x-shopify-hmac-sha256"]||"";
    if(!hmacOk(raw,hdr,process.env.SHOPIFY_WEBHOOK_SECRET||"")){ res.status(401).send("bad hmac"); return; }
    const order=JSON.parse(raw.toString("utf8"));
    const conv=await transformOrder(order);
    remember({
      id:order.id, name:order.name, currency:order.currency, total_price:toNum(order.total_price),
      email:order.email||"", shipping_address:order.shipping_address||{}, billing_address:order.billing_address||{},
      payload:conv, created_at:order.created_at||new Date().toISOString()
    });
    statusById.set(order.id,"awaiting_shipment");
    console.log("UPDATED MATCH",order.id,order.name,"items:",conv.after.length);
    res.status(200).send("ok");
  }catch(e){ console.error(e); res.status(500).send("err"); }
});

app.post("/webhooks/orders-cancelled", async (req,res)=>{
  try{
    const raw=await getRawBody(req);
    const hdr=req.headers["x-shopify-hmac-sha256"]||"";
    if(!hmacOk(raw,hdr,process.env.SHOPIFY_WEBHOOK_SECRET||"")){ res.status(401).send("bad hmac"); return; }
    const order=JSON.parse(raw.toString("utf8"));
    statusById.set(order.id,"cancelled");
    remember({ id:order.id, payload:{ after:[] } });
    console.log("CANCEL MATCH",order.id,order.name);
    res.status(200).send("ok");
  }catch(e){ console.error(e); res.status(500).send("err"); }
});

function minimalXML(o) {
  if (!o || !o.payload?.after?.length) return `<?xml version="1.0" encoding="utf-8"?><Orders></Orders>`;
  const items = o.payload.after.map(item => ({
    id: item.id || "UNKNOWN",
    title: item.title || "Item",
    sku: item.sku || "",
    qty: Math.max(1, parseInt(item.qty || 1, 10)),
    unitPrice: Number.isFinite(item.unitPrice) ? item.unitPrice : 0,
    imageUrl: item.imageUrl || ""
  }));
  const subtotal = items.reduce((s, i) => s + i.unitPrice * i.qty, 0);
  const tax = 0;
  const shipping = 0;
  const total = subtotal + tax + shipping;
  const bill = filledAddress(o.billing_address || {});
  const ship = filledAddress(o.shipping_address || {});
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

function authOK(req){
  const h=req.headers.authorization||"";
  if(h.startsWith("Basic ")){
    const [u,p]=Buffer.from(h.slice(6),"base64").toString("utf8").split(":",2);
    if(u===process.env.SS_USER && p===process.env.SS_PASS) return true;
  }
  const q=req.query||{};
  const u=q["SS-UserName"]||q["username"];
  const p=q["SS-Password"]||q["password"];
  return u===process.env.SS_USER && p===process.env.SS_PASS;
}

function shipstationHandler(req,res){
  res.set({"Content-Type":"application/xml; charset=utf-8"});
  if(!process.env.SS_USER||!process.env.SS_PASS){ res.status(503).send(`<?xml version="1.0" encoding="utf-8"?><Error>Auth not configured</Error>`); return; }
  if(!authOK(req)){ res.status(401).set("WWW-Authenticate","Basic").send(`<?xml version="1.0" encoding="utf-8"?><Error>Auth</Error>`); return; }
  const q=Object.fromEntries(Object.entries(req.query).map(([k,v])=>[k.toLowerCase(),String(v)]));
  const action=(q.action||"").toLowerCase();
  if(action==="test"||action==="status"){ res.status(200).send(`<?xml version="1.0" encoding="utf-8"?><Store><Status>OK</Status></Store>`); return; }
  if (q.order_id) {
    const wanted = String(q.order_id);
    const found = bestById.get(wanted) || history.slice().reverse().find(o => String(o.id) === wanted);
    res.status(200).send(minimalXML(found || null));
    return;
  }
  const xml = minimalXML(last());
  res.status(200).send(xml);
}

app.get("/shipstation", shipstationHandler);
app.post("/shipstation", shipstationHandler);
app.head("/shipstation", shipstationHandler);

app.get("/health",(req,res)=>res.send("ok"));
app.listen(process.env.PORT||8080, () => console.log("Server running on port", process.env.PORT||8080));
