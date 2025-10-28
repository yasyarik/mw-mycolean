import express from "express";
import dotenv from "dotenv";
import getRawBody from "raw-body";
import crypto from "crypto";

dotenv.config();
const app = express();

// === УТИЛИТЫ ===
function hmacOk(raw, header, secret) {
  if (!header || !secret) return false;
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
function sum(items){ return items.reduce((s,i)=>s+Number(i.unitPrice||0)*Number(i.qty||0),0); }
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

// === ХЕЛПЕРЫ ДЛЯ БАНДЛОВ (SKIO / AfterSell / SB) ===
function propVal(li, name) {
  const p = Array.isArray(li?.properties) ? li.properties : [];
  const f = p.find(x => x && x.name === name);
  return f ? String(f.value ?? "") : null;
}
function isBundleParent(li){
  const p=li.properties||[];
  if (p.find(x=>x.name==="_sb_parent" && String(x.value).toLowerCase()==="true")) return true;
  if (p.find(x=>x.name==="_bundle" || x.name==="bundle_id")) return true;
  if (p.find(x=>x.name==="skio_parent" && String(x.value).toLowerCase()==="true")) return true;
  return false;
}
function hasParentFlag(li) {
  const p = Array.isArray(li?.properties) ? li.properties : [];
  return p.some(x => {
    const n = String(x?.name || "").toLowerCase();
    const v = String(x?.value || "").toLowerCase();
    if (!n) return false;
    if (["_sb_parent", "skio_parent", "bundle_parent"].includes(n)) {
      return v === "true" || v === "1" || v === "yes";
    }
    return false;
  });
}
function anyAfterSellKey(li) {
  const p = Array.isArray(li?.properties) ? li.properties : [];
  for (const x of p) {
    const n = String(x?.name || "");
    if (/after[-_ ]?sell/i.test(n)) return true;
  }
  return false;
}
function bundleKey(li){
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
  if (!v && anyAfterSellKey(li)) {
    // произведём стабильный ключ на основе первой aftersell-свойства
    const first = (p.find(x => /after[-_ ]?sell/i.test(String(x?.name||""))) || {});
    const n = String(first.name||"aftersell");
    const val = String(first.value||"");
    v = `aftersell:${n}:${val}`;
  }
  return v;
}
function parseSbComponents(li) {
  const props = Array.isArray(li?.properties) ? li.properties : [];
  const rawVals = props
    .filter(p =>
      ["_sb_bundle_variant_id_qty", "_sb_components", "_sb_child_id_qty"].includes(p.name)
    )
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

// === КЭШИ/ХРАНИЛКИ ===
const history=[]; const last=()=>history.length?history[history.length-1]:null;
const statusById=new Map();
const variantPriceCache=new Map();
const variantImageCache = new Map();
const bestById = new Map(); // «лучшая» версия по order_id

function childrenCount(o){
  return Array.isArray(o?.payload?.after) ? o.payload.after.length : 0;
}
function remember(e){
  history.push(e);
  while(history.length>100) history.shift();
  const key = String(e.id);
  const prev = bestById.get(key);
  if (!prev || childrenCount(e) >= childrenCount(prev)) {
    bestById.set(key, e);
  }
}

// === API: цены/рецепт/картинки ===
async function fetchVariantPrice(variantId){
  const key=String(variantId);
  if(variantPriceCache.has(key)) return variantPriceCache.get(key);
  const shop=process.env.SHOPIFY_SHOP;
  const token=process.env.SHOPIFY_ADMIN_ACCESS_TOKEN;
  if(!shop||!token) { variantPriceCache.set(key,0); return 0; }
  const url=`https://${shop}/admin/api/2025-01/variants/${key}.json`;
  try{
    const r=await fetch(url,{headers:{"X-Shopify-Access-Token":token,"Content-Type":"application/json"}});
    if(!r.ok){ variantPriceCache.set(key,0); return 0; }
    const j=await r.json();
    const price=toNum(j?.variant?.price);
    variantPriceCache.set(key,price);
    return price;
  }catch(_){ variantPriceCache.set(key,0); return 0; }
}
async function fetchBundleRecipe(productId, variantId){
  const shop=process.env.SHOPIFY_SHOP;
  const token=process.env.SHOPIFY_ADMIN_ACCESS_TOKEN;
  if(!shop||!token||!productId) return null;
  const tryKeys=[
    {ns:"simple_bundles_2_0", key:"components"},
    {ns:"simple_bundles",     key:"components"},
    {ns:"simplebundles",      key:"components"},
    {ns:"bundles",            key:"components"},
  ];
  const gql = `
  query($pid: ID!, $vKeys: [HasMetafieldsIdentifier!]!) {
    product(id: $pid) {
      id
      metafields(identifiers: $vKeys){ namespace key value type }
    }
  }`;
  const identifiers = tryKeys.map(k=>({namespace:k.ns,key:k.key}));
  try{
    const r = await fetch(`https://${shop}/admin/api/2025-01/graphql.json`,{
      method:"POST",
      headers:{ "X-Shopify-Access-Token":token, "Content-Type":"application/json" },
      body: JSON.stringify({ query:gql, variables:{ pid:`gid://shopify/Product/${productId}`, vKeys:identifiers } })
    });
    if(!r.ok) return null;
    const j = await r.json();
    const mfs = j?.data?.product?.metafields || [];
    const mf = mfs.find(m=>m?.value);
    if(!mf) return null;
    let parsed=null;
    try{ parsed = JSON.parse(mf.value); }catch(_){}
    if(!parsed) return null;
    const list = Array.isArray(parsed) ? parsed : (Array.isArray(parsed.components)?parsed.components:[]);
    const out=[];
    for(const it of list){
      const vid = String(it.variantId || it.variant_id || "").replace(/^gid:\/\/shopify\/ProductVariant\//,"");
      const qty = Number(it.quantity || it.qty || it.qty_each || 0);
      if(vid && qty>0) out.push({ variantId: vid, qty });
    }
    return out.length ? out : null;
  }catch(_){ return null; }
}
async function getVariantImage(variantId) {
  if (!variantId) return "";
  const key = String(variantId);
  if (variantImageCache.has(key)) return variantImageCache.get(key) || "";

  const shop = process.env.SHOPIFY_SHOP;
  const token = process.env.SHOPIFY_ADMIN_ACCESS_TOKEN;
  if (!shop || !token){ variantImageCache.set(key,""); return ""; }

  const url = `https://${shop}/admin/api/2025-01/variants/${variantId}.json`;
  try {
    const res = await fetch(url, { headers: { "X-Shopify-Access-Token": token } });
    if (!res.ok){ variantImageCache.set(key,""); return ""; }
    const data = await res.json();
    const variant = data.variant;
    let imageUrl = variant?.image?.src || "";
    if (!imageUrl && variant?.product_id) {
      const prodRes = await fetch(
        `https://${shop}/admin/api/2025-01/products/${variant.product_id}.json?fields=images`,
        { headers: { "X-Shopify-Access-Token": token } }
      );
      if (prodRes.ok) {
        const p = await prodRes.json();
        imageUrl = p.product?.images?.[0]?.src || "";
      }
    }
    variantImageCache.set(key, imageUrl || "");
    return imageUrl || "";
  } catch (_) {
    variantImageCache.set(key,"");
    return "";
  }
}

// === SIMPLE BUNDLES DETECT (расширено) ===
function sbDetectFromOrder(order){
  const items = Array.isArray(order.line_items) ? order.line_items : [];
  const tagStr = String(order.tags || "").toLowerCase();
  if (!items.length) return null;

  const epsilon = 0.00001;
  const daSum = li => (Array.isArray(li.discount_allocations) ? li.discount_allocations.reduce((s,d)=>s+toNum(d.amount),0) : 0);
  const zeroed = li => (daSum(li) >= toNum(li.price) - epsilon) || (toNum(li.total_discount) >= toNum(li.price) - epsilon);

  // группируем по ключам (SB/SKIO/AfterSell)
  const groups = new Map();
  for (const li of items) {
    const k = bundleKey(li);
    if (!k) continue;
    if (!groups.has(k)) groups.set(k, []);
    groups.get(k).push(li);
  }
  const childrenSet = new Set();

  for (const arr of groups.values()) {
    if (arr.length < 2) continue;

    const zeroKids = arr.filter(zeroed);
    if (zeroKids.length) { zeroKids.forEach(li => childrenSet.add(li)); continue; }

    if (arr.some(hasParentFlag)) {
      arr.forEach(li => { if (!hasParentFlag(li)) childrenSet.add(li); });
      continue;
    }

    const withPrice = arr.map(li => ({ li, p: toNum(li.price) }));
    const maxP = Math.max(...withPrice.map(x => x.p));
    const parentGuess = withPrice.find(x => x.p === maxP)?.li || null;
    arr.forEach(li => { if (li !== parentGuess) childrenSet.add(li); });
  }

  if (childrenSet.size > 0) {
    return { parent: null, children: Array.from(childrenSet) };
  }

  // базовый SB-детект как раньше
  const children = items.filter(li => zeroed(li));
  const parents = items.filter(li => !zeroed(li));

  if (children.length && parents.length === 1) {
    return { parent: parents[0], children };
  }
  if (children.length && tagStr.includes("simple bundles")) {
    return { parent: null, children };
  }

  const withPrice = items.filter(li => toNum(li.price) > 0);
  const maxPrice = withPrice.length ? Math.max(...withPrice.map(li => toNum(li.price))) : 0;
  const parentGuess = withPrice.find(li => toNum(li.price) === maxPrice) || null;
  if (tagStr.includes("simple bundles") && parentGuess && items.length > 1) {
    const rest = items.filter(li => li !== parentGuess);
    return { parent: parentGuess, children: rest };
  }
  return null;
}

// === PUSH LINE (c картинкой) ===
async function pushChildLine(after, li, unitPrice=null){
  const imageUrl = await getVariantImage(li.variant_id || li.variantId || null);
  after.push({
    id: li.id,
    title: li.title,
    sku: li.sku || null,
    qty: li.quantity || 1,
    unitPrice: unitPrice!=null ? toNum(unitPrice) : toNum(li.price),
    imageUrl
  });
}

// === ТРАНСФОРМ ===
async function transformOrder(order){
  const groups={};
  for(const li of order.line_items||[]){
    const k=bundleKey(li); if(!k) continue;
    if(!groups[k]) groups[k]={parent:null,children:[]};
    if(isBundleParent(li)) groups[k].parent=li; else groups[k].children.push(li);
  }
  const after=[]; const handled=new Set();

  // 1) Готовые группы (SB/SKIO/AfterSell)
  for(const [k,g] of Object.entries(groups)){
    const parent=g.parent, kids=g.children;
    if(parent) handled.add(parent.id); for(const c of kids) handled.add(c.id);

    if(parent && kids.length>0){
      const totalParent=toNum(parent.price)* (parent.quantity||1);
      const pricedKids=[]; const zeroKids=[];
      for(const c of kids){ const p=toNum(c.price); (p>0?pricedKids:zeroKids).push(p>0?{c,p}:c); }

      if(zeroKids.length){
        for(const zk of zeroKids){
          let vp=0; const vid=zk.variant_id||zk.variantId||null;
          if(vid) vp=await fetchVariantPrice(vid);
          pricedKids.push({c:zk,p:toNum(vp)});
        }
      }
      if(pricedKids.some(x=>x.p>0)){
        for(const {c,p} of pricedKids){
          await pushChildLine(after, c, p);
        }
      }else{
        let qtySum=kids.reduce((s,c)=>s+(c.quantity||0),0);
        qtySum=Math.max(1,qtySum);
        let rest=Math.round(totalParent*100);
        for(let i=0;i<kids.length;i++){
          const c=kids[i];
          const share=i===kids.length-1?rest:Math.round((totalParent*((c.quantity||0)/qtySum))*100);
          rest-=share;
          const unit=(c.quantity||1)>0?share/(c.quantity||1)/100:0;
          await pushChildLine(after, c, Number(unit.toFixed(2)));
        }
      }
    }
  }

  // 2) SB-детект (дети — только дети)
  const sb = sbDetectFromOrder(order);
  if (sb) {
    const { parent, children } = sb;
    if (parent) handled.add(parent.id);
    for (const c of children) {
      handled.add(c.id);
      await pushChildLine(after, c, null);
    }
  }

  // 3) Если нет групп/детектов — парсим компоненты в свойствах позиции
  const hasAnyGroup = Object.keys(groups).length > 0;
  if (!hasAnyGroup && !sb){
    for (const li of (order.line_items||[])) {
      const comps = parseSbComponents(li);
      if (!comps.length) continue;

      const priced=[]; let anyPrice=false;
      for(const c of comps){
        const vp=await fetchVariantPrice(c.variantId);
        const price=toNum(vp);
        if(price>0) anyPrice=true;
        priced.push({variantId:c.variantId, qty:c.qty, price});
      }
      if(anyPrice){
        for(const x of priced){
          const imageUrl = await getVariantImage(x.variantId);
          after.push({ id:`${li.id}::${x.variantId}`, title:li.title, sku:li.sku||null, qty:x.qty, unitPrice:toNum(x.price), imageUrl });
        }
      }else{
        const total=toNum(li.price)*(li.quantity||1);
        const qtySum=Math.max(1, comps.reduce((s,c)=>s+(c.qty||0),0));
        let rest=Math.round(total*100);
        for(let i=0;i<comps.length;i++){
          const c=comps[i];
          const share=i===comps.length-1?rest:Math.round((total*((c.qty||0)/qtySum))*100);
          rest-=share;
          const unit=(c.qty||1)>0?share/(c.qty||1)/100:0;
          const imageUrl = await getVariantImage(c.variantId);
          after.push({ id:`${li.id}::${c.variantId}`, title:li.title, sku:li.sku||null, qty:c.qty||1, unitPrice:Number(unit.toFixed(2)), imageUrl });
        }
      }
      handled.add(li.id);
    }
  }

  // 4) Если вообще ничего не собралось — положим исходные позиции как есть (дети/обычные)
  if (after.length===0){
    for(const li of (order.line_items||[])){
      const base = toNum(li.price);
      const fetched = li.variant_id ? await fetchVariantPrice(li.variant_id) : 0;
      const price = base>0 ? base : toNum(fetched);
      const imageUrl = await getVariantImage(li.variant_id);
      after.push({
        id: li.id,
        title: li.title,
        sku: li.sku || null,
        qty: li.quantity || 1,
        unitPrice: price,
        imageUrl
      });
    }
  }

  return { after };
}

// === ОБРАБОТЧИКИ ВЕБХУКОВ ===
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
      currency: order.currency,
      total_price: toNum(order.total_price),
      email: order.email || "",
      shipping_address: order.shipping_address || {},
      billing_address: order.billing_address || {},
      payload: conv,
      created_at: order.created_at || new Date().toISOString()
    });
    statusById.set(order.id, "awaiting_shipment");
    console.log("ORDER PROCESSED", order.id, "#", order.name, "children:", conv.after.length);
    res.status(200).send("ok");
  } catch (e) {
    console.error("Webhook error:", e);
    res.status(500).send("error");
  }
}
app.post("/webhooks/orders-create", handleOrderCreateOrUpdate);
app.post("/webhooks/orders-updated", handleOrderCreateOrUpdate);

app.post("/webhooks/orders-cancelled", async (req,res)=>{
  try{
    const raw=await getRawBody(req);
    const hdr=req.headers["x-shopify-hmac-sha256"]||"";
    if(!hmacOk(raw,hdr,process.env.SHOPIFY_WEBHOOK_SECRET||"")){ res.status(401).send("bad hmac"); return; }
    const order=JSON.parse(raw.toString("utf8"));
    statusById.set(order.id,"cancelled");
    if(!last() || last().id!==order.id){
      remember({
        id:order.id, name:order.name, currency:order.currency, total_price:toNum(order.total_price),
        email:order.email||"", shipping_address:order.shipping_address||{}, billing_address:order.billing_address||{},
        payload:{ after:[] }, created_at:order.created_at||new Date().toISOString()
      });
    }
    console.log("ORDER CANCELLED",order.id);
    res.status(200).send("ok");
  }catch(e){ console.error("Cancel webhook error:",e); res.status(500).send("error"); }
});

// === XML ДЛЯ SHIPSTATION (с картинками) ===
function minimalXML(o){
  if(!o || !o.payload?.after?.length) return `<?xml version="1.0" encoding="utf-8"?><Orders></Orders>`;
  const items = o.payload.after.map(i => ({
    id: i.id || "UNKNOWN",
    title: i.title || "Item",
    sku: i.sku || "",
    qty: Math.max(1, parseInt(i.qty || 1, 10)),
    unitPrice: Number.isFinite(i.unitPrice) ? i.unitPrice : 0,
    imageUrl: i.imageUrl || ""
  }));
  const subtotal = items.reduce((s, i) => s + i.unitPrice * i.qty, 0);
  const tax = 0;
  const shipping = 0;
  const total = subtotal + tax + shipping;

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
    <OrderNumber>${esc((o.name || String(o.id)).replace(/^#/, ''))}</OrderNumber>
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

// === SHIPSTATION ХЕНДЛЕР (test/status, order_id, sample, strict dates) ===
function authOK(req){
  const h=req.headers.authorization||"";
  if(h.startsWith("Basic ")){
    const [u,p]=Buffer.from(h.slice(6),"base64").toString("utf8").split(":",2);
    if(u===process.env.SS_USER && p===process.env.SS_PASS) return true;
  }
  const q=req.query||{};
  const u=q["SS-UserName"]||q["username"]||q["ss-username"];
  const p=q["SS-Password"]||q["password"]||q["ss-password"];
  return u===process.env.SS_USER && p===process.env.SS_PASS;
}
function buildSampleOrder(){
  const now=new Date().toISOString();
  return {
    id:999000111, name:"#SAMPLE", currency:"USD", email:"sample@mycolean.com",
    shipping_address:{first_name:"Sample",last_name:"Buyer",address1:"1 Sample Street",city:"Austin",province_code:"TX",zip:"73301",country_code:"US"},
    billing_address:{first_name:"Sample",last_name:"Buyer",address1:"1 Sample Street",city:"Austin",province_code:"TX",zip:"73301",country_code:"US"},
    payload:{ after:[{id:"S1",title:"Mycolean Classic 4-Pack",sku:"MYCO-4PK",qty:1,unitPrice:49.95,imageUrl:"https://via.placeholder.co/300"}] },
    total_price:49.95, created_at:now
  };
}
function shipstationHandler(req,res){
  res.set({"Content-Type":"application/xml; charset=utf-8","Cache-Control":"no-store, no-cache, must-revalidate, max-age=0","Pragma":"no-cache","Expires":"0"});
  if(!process.env.SS_USER||!process.env.SS_PASS){ res.status(503).send(`<?xml version="1.0" encoding="utf-8"?><Error>Auth not configured</Error>`); return; }
  if(!authOK(req)){ res.status(401).set("WWW-Authenticate","Basic").send(`<?xml version="1.0" encoding="utf-8"?><Error>Auth</Error>`); return; }

  const q=Object.fromEntries(Object.entries(req.query).map(([k,v])=>[k.toLowerCase(),String(v)]));
  const action=(q.action||"").toLowerCase();
  if(action==="test"||action==="status"){ res.status(200).send(`<?xml version="1.0" encoding="utf-8"?><Store><Status>OK</Status></Store>`); return; }

  if (q.order_id) {
    const wanted = String(q.order_id);
    const best = bestById.get(wanted);
    const found = best || history.slice().reverse().find(o => String(o.id) === wanted);
    res.status(200).send(minimalXML(found || null));
    return;
  }

  let o=last();
  if(!o && String(process.env.SS_SAMPLE_ON_EMPTY||"").toLowerCase()==="true"){ o=buildSampleOrder(); }

  const strict=String(process.env.SS_STRICT_DATES||"").toLowerCase()==="true";
  if(o && strict){
    const start=q.start_date?Date.parse(q.start_date):null;
    const end=q.end_date?Date.parse(q.end_date):null;
    if((start && Date.parse(o.created_at)<start) || (end && Date.parse(o.created_at)>end)){ o=null; }
  }

  res.status(200).send(minimalXML(o||null));
}
app.get("/shipstation", shipstationHandler);
app.post("/shipstation", shipstationHandler);
app.head("/shipstation", shipstationHandler);

// === HEALTH ===
app.get("/health",(req,res)=>res.send("ok"));

// === ЗАПУСК ===
app.listen(process.env.PORT||8080, () => console.log("Server running on", process.env.PORT||8080));
