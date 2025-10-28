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
function sum(items){ return items.reduce((s,i)=>s+Number(i.unitPrice||0)*Number(i.qty||0),0); }

function isBundleParent(li){
  const p=li.properties||[];
  if (p.find(x=>x.name==="_sb_parent" && String(x.value).toLowerCase()==="true")) return true;
  if (p.find(x=>x.name==="_bundle" || x.name==="bundle_id")) return true;
  if (p.find(x=>x.name==="skio_parent" && String(x.value).toLowerCase()==="true")) return true;
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
function isSubscription(order) {
  const items = Array.isArray(order?.line_items) ? order.line_items : [];
  if (items.some(it => it?.selling_plan_id || it?.selling_plan_allocation?.selling_plan_id)) return true;
  const tags = String(order?.tags || "").toLowerCase();
  if (tags.includes("subscription")) return true;
  const SKIO_APP_ID = 580111;
  if (order?.app_id === SKIO_APP_ID) return true;
  return false;
}
function looksLikeSimpleBundles(order){
  const tags = String(order?.tags||"").toLowerCase();
  const items = Array.isArray(order?.line_items) ? order.line_items : [];
  const hasProp = items.some(li =>
    Array.isArray(li.properties) &&
    li.properties.some(p => ["_sb_bundle_variant_id_qty","_sb_components","_sb_child_id_qty"].includes(p.name))
  );
  return tags.includes("simple bundles") || tags.includes("simple bundles 2.0") || hasProp;
}
function pushLine(after, li, { unitPrice = null, parent = false, key = null, imageUrl = null } = {}){
  const price = unitPrice != null ? unitPrice : toNum(li.price);
  after.push({
    id: li.id,
    title: li.title,
    sku: li.sku || null,
    qty: li.quantity || 1,
    unitPrice: price,
    parent,
    key,
    imageUrl: imageUrl || null
  });
}

const history=[]; const last=()=>history.length?history[history.length-1]:null;
const statusById=new Map();

const variantInfoCache=new Map();
const imageUrlCache=new Map();

async function fetchVariantInfo(variantId){
  const key=String(variantId);
  if(variantInfoCache.has(key)) return variantInfoCache.get(key);
  const shop=process.env.SHOPIFY_SHOP;
  const token=process.env.SHOPIFY_ADMIN_ACCESS_TOKEN;
  if(!shop||!token) return { price:0, productId:null, imageId:null, title:"", sku:"" };
  const url=`https://${shop}/admin/api/2025-01/variants/${key}.json`;
  const r=await fetch(url,{headers:{"X-Shopify-Access-Token":token,"Content-Type":"application/json"}});
  if(!r.ok) { const v={price:0,productId:null,imageId:null,title:"",sku:""}; variantInfoCache.set(key,v); return v; }
  const j=await r.json();
  const v = {
    price: toNum(j?.variant?.price),
    productId: j?.variant?.product_id||null,
    imageId: j?.variant?.image_id||null,
    title: j?.variant?.title || "",
    sku: (j?.variant?.sku || "").trim()
  };
  variantInfoCache.set(key,v);
  return v;
}

async function fetchImageUrl(productId, imageId){
  if(imageId){
    const ik=`${productId}:${imageId}`;
    if(imageUrlCache.has(ik)) return imageUrlCache.get(ik);
    const shop=process.env.SHOPIFY_SHOP;
    const token=process.env.SHOPIFY_ADMIN_ACCESS_TOKEN;
    if(!shop||!token){ imageUrlCache.set(ik,""); return ""; }
    const url=`https://${shop}/admin/api/2025-01/products/${productId}/images/${imageId}.json`;
    const r=await fetch(url,{headers:{"X-Shopify-Access-Token":token,"Content-Type":"application/json"}});
    if(!r.ok){ imageUrlCache.set(ik,""); return ""; }
    const j=await r.json();
    const src=String(j?.image?.src||"");
    imageUrlCache.set(ik,src);
    return src;
  }
  const pk=String(productId||"");
  if(imageUrlCache.has(pk)) return imageUrlCache.get(pk);
  const shop=process.env.SHOPIFY_SHOP;
  const token=process.env.SHOPIFY_ADMIN_ACCESS_TOKEN;
  if(!shop||!token){ imageUrlCache.set(pk,""); return ""; }
  const url=`https://${shop}/admin/api/2025-01/products/${productId}.json`;
  const r=await fetch(url,{headers:{"X-Shopify-Access-Token":token,"Content-Type":"application/json"}});
  if(!r.ok){ imageUrlCache.set(pk,""); return ""; }
  const j=await r.json();
  const imgs=j?.product?.images||[];
  const first=imgs.length?String(imgs[0].src||""):"";
  imageUrlCache.set(pk,first);
  return first;
}

async function fetchVariantPrice(variantId){
  const info=await fetchVariantInfo(variantId);
  return info.price||0;
}

const bestById = new Map();
function childrenCount(o){
  return Array.isArray(o?.payload?.after) ? o.payload.after.filter(i => !i.parent).length : 0;
}
function remember(e){
  history.push(e);
  while(history.length>100) history.shift();
  const key = String(e.id);
  const prev = bestById.get(key);
  if (!prev || childrenCount(e) > childrenCount(prev)) {
    bestById.set(key, e);
  }
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
}

async function transformOrder(order){
  const after=[]; const handled=new Set();

  if (looksLikeSimpleBundles(order)) {
    const items = Array.isArray(order.line_items) ? order.line_items : [];
    const seed = items.find(li =>
      Array.isArray(li.properties) &&
      li.properties.some(p => ["_sb_bundle_variant_id_qty","_sb_components","_sb_child_id_qty"].includes(p.name)
    )) || null;

    if (seed) {
      const gkey = bundleKey(seed);
      const group = gkey ? items.filter(li => bundleKey(li) === gkey) : [seed];

      const union = new Map();
      for (const li of group) {
        const part = parseSbComponents(li);
        for (const c of part) {
          const q = toNum(c.qty);
          if (!c.variantId || q <= 0) continue;
          union.set(c.variantId, (union.get(c.variantId) || 0) + q);
        }
      }

      const comps = Array.from(union.entries()).map(([variantId, qty]) => ({ variantId, qty }));
      if (comps.length) {
        const baseQty = Math.max(1, group.reduce((s, li) => s + toNum(li.quantity||0), 0)) || toNum(seed.quantity||1);
        const key = gkey || `_sb_${seed.id}`;

        for (const li of group) handled.add(li.id);

        for (const c of comps) {
          const info = await fetchVariantInfo(c.variantId);
          const imageUrl = await fetchImageUrl(info.productId, info.imageId);
          const qty = Math.max(1, toNum(c.qty) * (baseQty || 1));
          after.push({
            id: `${seed.id}::${c.variantId}`,
            title: info.title ? `${seed.title} — ${info.title}` : seed.title,
            sku: info.sku || seed.sku || null,
            qty,
            unitPrice: toNum(info.price || 0),
            parent: false,
            key,
            imageUrl
          });
        }
      }
    }
  }

  if (after.length === 0) {
    const groups={};
    for(const li of order.line_items||[]){
      const k=bundleKey(li); if(!k) continue;
      if(!groups[k]) groups[k]={parent:null,children:[]};
      if(isBundleParent(li)) groups[k].parent=li; else groups[k].children.push(li);
    }
    for(const [k,g] of Object.entries(groups)){
      const parent=g.parent, kids=g.children;
      if(parent) handled.add(parent.id); for(const c of kids) handled.add(c.id);
      if(parent && kids.length>0){
        const totalParent=toNum(parent.price)* (parent.quantity||1);
        const pricedKids=[]; const zeroKids=[];
        for(const c of kids){ const p=toNum(c.price); (p>0?pricedKids:zeroKids).push(p>0?{c,p}:c); }
        if(zeroKids.length){
          for(const zk of zeroKids){
            let vp=0; let img="";
            const vid=zk.variant_id||zk.variantId||null;
            if(vid){ const info=await fetchVariantInfo(vid); vp=toNum(info.price); img=await fetchImageUrl(info.productId, info.imageId); }
            pricedKids.push({c:zk,p:toNum(vp),img});
          }
        }
        if(pricedKids.some(x=>x.p>0)){
          for(const {c,p,img} of pricedKids){
            let imageUrl=img||"";
            if(!imageUrl && c.variant_id){ const info=await fetchVariantInfo(c.variant_id); imageUrl=await fetchImageUrl(info.productId, info.imageId); }
            after.push({id:c.id,title:c.title,sku:c.sku||null,qty:c.quantity||1,unitPrice:toNum(p),parent:false,key:k,imageUrl});
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
            let imageUrl="";
            if(c.variant_id){ const info=await fetchVariantInfo(c.variant_id); imageUrl=await fetchImageUrl(info.productId, info.imageId); }
            after.push({id:c.id,title:c.title,sku:c.sku||null,qty:c.quantity||1,unitPrice:Number(unit.toFixed(2)),parent:false,key:k,imageUrl});
          }
        }
      }
    }

    if (after.length===0){
      for (const li of (order.line_items||[])) {
        const comps = parseSbComponents(li);
        if (!comps.length) continue;
        const priced=[]; let anyPrice=false;
        for(const c of comps){
          const info=await fetchVariantInfo(c.variantId);
          const price=toNum(info.price);
          if(price>0) anyPrice=true;
          priced.push({variantId:c.variantId, qty:c.qty, price, info});
        }
        if(anyPrice){
          for(const x of priced){
            const imageUrl=await fetchImageUrl(x.info.productId, x.info.imageId);
            after.push({ id:`${li.id}::${x.variantId}`, title:li.title, sku:x.info.sku||li.sku||null, qty:x.qty, unitPrice:toNum(x.price), parent:false, key:bundleKey(li)||`_sb_${li.id}`, imageUrl });
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
            const info=await fetchVariantInfo(c.variantId);
            const imageUrl=await fetchImageUrl(info.productId, info.imageId);
            after.push({ id:`${li.id}::${c.variantId}`, title:li.title, sku:info.sku||li.sku||null, qty:c.qty||1, unitPrice:Number(unit.toFixed(2)), parent:false, key:bundleKey(li)||`_sb_${li.id}`, imageUrl });
          }
        }
        handled.add(li.id);
      }
    }
  }

  for(const li of (order.line_items||[])){
    if(handled.has(li.id)) continue;
    const info = li.variant_id ? await fetchVariantInfo(li.variant_id) : {productId:null,imageId:null,sku:li.sku||""};
    const imageUrl = await fetchImageUrl(info.productId, info.imageId);
    after.push({
      id:li.id,
      title:li.title,
      sku:info.sku || li.sku || null,
      qty:li.quantity||1,
      unitPrice:toNum(li.price),
      parent:false,
      key:bundleKey(li),
      imageUrl
    });
  }

  if (after.length===0){
    for(const li of (order.line_items||[])){
      const info = li.variant_id ? await fetchVariantInfo(li.variant_id) : {productId:null,imageId:null,sku:li.sku||""};
      const imageUrl = await fetchImageUrl(info.productId, info.imageId);
      after.push({
        id: li.id,
        title: li.title,
        sku: info.sku || li.sku || null,
        qty: li.quantity || 1,
        unitPrice: toNum(li.price),
        parent: false,
        key: bundleKey(li),
        imageUrl
      });
    }
  }

  return { after };
}

function minimalXML(o){
  if(!o) return `<?xml version="1.0" encoding="utf-8"?><Orders></Orders>`;
  let children=(o.payload?.after||[]).filter(i=>!i.parent);
  if(!children.length){ children=[{id:"FALLBACK",title:"Bundle",sku:"BUNDLE",qty:1,unitPrice:Number(o.total_price||0),imageUrl:""}]; }
  const subtotal=sum(children), tax=0, shipping=0, total=subtotal+tax+shipping;
  const bill=filledAddress(o.billing_address||{}), ship=filledAddress(o.shipping_address||{});
  const email=(o.email&&o.email.includes("@"))?o.email:"customer@example.com";
  const orderDate=fmtShipDate(new Date(o.created_at||Date.now())), lastMod=fmtShipDate(new Date());
  const status=statusById.get(o.id) || "awaiting_shipment";
  const itemsXml=children.map(i=>`
      <Item>
        <LineItemID>${esc(String(i.id||""))}</LineItemID>
        <SKU>${esc(skuSafe(i,o.id))}</SKU>
        <Name>${esc(i.title||"Item")}</Name>
        <Quantity>${Math.max(1,parseInt(i.qty||0,10))}</Quantity>
        <UnitPrice>${money2(Number.isFinite(i.unitPrice)?i.unitPrice:0)}</UnitPrice>
        <Adjustment>false</Adjustment>
        <ImageUrl>${esc(String(i.imageUrl||""))}</ImageUrl>
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
    payload:{ after:[{id:"S1",title:"Mycolean Classic 4-Pack",sku:"MYCO-4PK",qty:1,unitPrice:49.95,parent:false,imageUrl:""}] },
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
    const xml = minimalXML(found || null);
    res.status(200).send(xml);
    return;
  }
  let o = null;
  {
    const all = Array.from(bestById.values());
    if (all.length) {
      all.sort((a, b) => {
        const ca = childrenCount(a), cb = childrenCount(b);
        if (cb !== ca) return cb - ca;
        const ta = Date.parse(a.created_at||0), tb = Date.parse(b.created_at||0);
        return tb - ta;
      });
      o = all[0];
    } else {
      o = last() || null;
    }
  }
  if(!o && String(process.env.SS_SAMPLE_ON_EMPTY||"").toLowerCase()==="true"){ o=buildSampleOrder(); }
  const strict=String(process.env.SS_STRICT_DATES||"").toLowerCase()==="true";
  if(o && strict){
    const start=q.start_date?Date.parse(q.start_date):null;
    const end=q.end_date?Date.parse(q.end_date):null;
    if((start && Date.parse(o.created_at)<start) || (end && Date.parse(o.created_at)>end)){ o=null; }
  }
  const xml=minimalXML(o);
  res.status(200).send(xml);
}

app.get("/shipstation", shipstationHandler);
app.post("/shipstation", shipstationHandler);
app.head("/shipstation", shipstationHandler);

app.get("/health",(req,res)=>res.send("ok"));
app.listen(process.env.PORT||8080);
