import express from "express";
import dotenv from "dotenv";
import getRawBody from "raw-body";
import crypto from "crypto";

dotenv.config();
const app = express();

// === УТИЛИТЫ ===
function hmacOk(raw, header, secret) {
  if (!header) return false;
  const sig = crypto.createHmac("sha256", secret).update(raw).digest("base64");
  const a = Buffer.from(header), b = Buffer.from(sig);
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

function toNum(x){ return Number.isFinite(x)?x:0; }
function esc(x=""){ return String(x).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;"); }
function money2(v){ return (Number.isFinite(v)?v:0).toFixed(2); }
function filledAddress(a){
  const d=v=>(v&&String(v).trim())?String(v):"";
  const name=[d(a.first_name),d(a.last_name)].filter(Boolean).join(" ")||"Customer";
  return {
    name, company:d(a.company)||"", phone:d(a.phone)||"", email:d(a.email)||"",
    address1:d(a.address1)||"Address line 1", address2:d(a.address2)||"",
    city:d(a.city)||"City", state:(d(a.province_code)||"ST").slice(0,2).toUpperCase(),
    zip:d(a.zip)||"00000", country:(d(a.country_code)||"US").slice(0,2).toUpperCase()
  };
}
function skuSafe(i, orderId){
  const s=(i.sku||"").trim(); if(s) return s;
  return `MW-${orderId}-${i.id || "ITEM"}`;
}

// === SB DETECT ===
function sbDetectFromOrder(order){
  const items = Array.isArray(order.line_items) ? order.line_items : [];
  if (!items.length) return null;

  const epsilon = 0.00001;
  const zeroed = li => {
    const da = (Array.isArray(li.discount_allocations) ? li.discount_allocations.reduce((s,d)=>s+toNum(d.amount),0) : 0);
    return (da >= toNum(li.price) - epsilon) || (toNum(li.total_discount) >= toNum(li.price) - epsilon);
  };
  const children = items.filter(zeroed);
  const parents = items.filter(li => !zeroed(li));

  if (children.length && parents.length === 1) return { children };
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

// === КЭШИ ===
const history = [];
const bestById = new Map();
const statusById = new Map();

function remember(e){
  history.push(e);
  while(history.length>100) history.shift();
  bestById.set(String(e.id), e);
}

// === TRANSFORM ORDER (БЕЗ API) ===
async function transformOrder(order){
  const after = [];
  const handled = new Set();

  const sb = sbDetectFromOrder(order);
  if (sb) {
    for (const c of sb.children) {
      handled.add(c.id);
      let imageUrl = "";
      if (c.product_id) {
        const pid = String(c.product_id).padStart(10, '0');
        imageUrl = `https://cdn.shopify.com/s/files/1/0000/0000/${pid}_500x.jpg`;
      }
      pushLine(after, c, imageUrl);
      console.log("SB CHILD", { id: c.id, title: c.title, product_id: c.product_id, imageUrl: imageUrl ? "OK" : "NO" });
    }
    return { after };
  }

  for(const li of (order.line_items||[])){
    if(handled.has(li.id)) continue;
    let imageUrl = "";
    if (li.product_id) {
      const pid = String(li.product_id).padStart(10, '0');
      imageUrl = `https://cdn.shopify.com/s/files/1/0000/0000/${pid}_500x.jpg`;
    }
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

// === ВЕБХУКИ ===
app.post("/webhooks/orders-create", async (req,res)=>{
  try{
    const raw=await getRawBody(req);
    const hdr=req.headers["x-shopify-hmac-sha256"]||"";
    if(!hmacOk(raw,hdr,process.env.SHOPIFY_WEBHOOK_SECRET||"")){ res.status(401).send("bad"); return; }
    const order=JSON.parse(raw.toString("utf8"));
    const conv=await transformOrder(order);
    remember({
      id:order.id, name:order.name, email:order.email||"", 
      shipping_address:order.shipping_address||{}, billing_address:order.billing_address||{},
      payload:conv, created_at:order.created_at||new Date().toISOString()
    });
    statusById.set(order.id,"awaiting_shipment");
    console.log("ORDER", order.id, "items:", conv.after.length);
    res.status(200).send("ok");
  }catch(e){ console.error(e); res.status(500).send("err"); }
});

app.post("/webhooks/orders-updated", app.post("/webhooks/orders-create"));
app.post("/webhooks/orders-cancelled", async (req,res)=>{
  try{
    const raw=await getRawBody(req);
    const hdr=req.headers["x-shopify-hmac-sha256"]||"";
    if(!hmacOk(raw,hdr,process.env.SHOPIFY_WEBHOOK_SECRET||"")){ res.status(401).send("bad"); return; }
    const order=JSON.parse(raw.toString("utf8"));
    statusById.set(order.id,"cancelled");
    res.status(200).send("ok");
  }catch(e){ res.status(500).send("err"); }
});

// === XML ===
function minimalXML(o) {
  if (!o || !o.payload?.after?.length) return `<?xml version="1.0" encoding="utf-8"?><Orders></Orders>`;

  const items = o.payload.after.map(i => ({
    id: i.id, title: i.title, sku: i.sku || "", qty: i.qty, unitPrice: i.unitPrice, imageUrl: i.imageUrl || ""
  }));

  const total = items.reduce((s,i)=>s+i.unitPrice*i.qty,0);
  const bill = filledAddress(o.billing_address || {});
  const ship = filledAddress(o.shipping_address || {});
  const email = (o.email && o.email.includes("@")) ? o.email : "customer@example.com";

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
    <OrderNumber>${esc(o.name)}</OrderNumber>
    <OrderDate>${new Date().toISOString().slice(0,19).replace("T"," ")}</OrderDate>
    <OrderStatus>awaiting_shipment</OrderStatus>
    <LastModified>${new Date().toISOString().slice(0,19).replace("T"," ")}</LastModified>
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

// === SHIPSTATION ===
function authOK(req){
  const h=req.headers.authorization||"";
  if(h.startsWith("Basic ")){
    const [u,p]=Buffer.from(h.slice(6),"base64").toString("utf8").split(":",2);
    if(u===process.env.SS_USER && p===process.env.SS_PASS) return true;
  }
  return false;
}

app.use("/shipstation", (req,res)=>{
  res.set("Content-Type","application/xml");
  if(!authOK(req)){ res.status(401).send("<Error>Auth</Error>"); return; }
  const id = req.query.order_id;
  const order = id ? bestById.get(String(id)) : history[history.length-1];
  res.send(minimalXML(order || {payload:{after:[]}}));
});

app.get("/health", (req,res)=>res.send("ok"));
app.listen(process.env.PORT||8080, () => console.log("RUNNING"));
