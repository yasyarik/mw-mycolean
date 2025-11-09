import express from "express";
import dotenv from "dotenv";
import getRawBody from "raw-body";
import crypto from "crypto";

dotenv.config();
const app = express();
app.set("etag", false);

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
    return n.includes("aftersell") || n.includes("after_sell") || n.includes("post_purchase") || n.includes("upcart");
  });
}

function looksLikeBundle(li) {
  const t = (li.title || "").toLowerCase();
  const s = (li.sku || "").toLowerCase();
  return t.includes("bundle") || t.includes("pack") || s.includes("bundle") || s.includes("pack");
}

function detectSubBundleNoChildren(order){
  const items = Array.isArray(order?.line_items) ? order.line_items : [];
  if (!items.length) return null;
  const tagStr = String(order.tags || "").toLowerCase();
  const sb = sbDetectFromOrder(order);
  if (sb && Array.isArray(sb.children) && sb.children.length) return null;
  const looksBundle = (li) => {
    const t = `${li.title || ""} ${li.sku || ""}`.toLowerCase();
    if (t.includes("bundle") || t.includes("pack")) return true;
    const props = Array.isArray(li.properties) ? li.properties : [];
    return props.some(p => {
      const n = String(p?.name || "").toLowerCase();
      return n.includes("_sb_") || n.includes("bundle");
    });
  };
  const isSubscription = (li) => !!(li.selling_plan_id || li.selling_plan_allocation?.selling_plan_id);
  const parents = items.filter(li => isSubscription(li) && looksBundle(li));
  if (parents.length === 1 && items.length === 1) return parents[0];
  if (parents.length === 1 && tagStr.includes("simple bundles")) return parents[0];
  return null;
}

function hasSubUpgradeProp(li) {
  const props = Array.isArray(li.properties) ? li.properties : [];
  return props.some(p => String(p?.name || "").toLowerCase().includes("__upcartsubscriptionupgrade"));
}

function detectSubBundleParentOnly(order) {
  const items = Array.isArray(order?.line_items) ? order.line_items : [];
  if (!items.length) return [];
  const tags = String(order?.tags || "").toLowerCase();
  const isSubscriptionOrder = tags.includes("subscription");
  const out = [];
  for (const li of items) {
    const subFlag = isSubscriptionOrder || hasSubUpgradeProp(li) || li?.selling_plan_id || li?.selling_plan_allocation?.selling_plan_id;
    if (!subFlag) continue;
    if (!looksLikeBundle(li)) continue;
    out.push({
      id: li.id,
      title: li.title || "",
      sku: li.sku || null,
      variant_id: li.variant_id || null,
      product_id: li.product_id || null,
      quantity: li.quantity || 1
    });
  }
  return out;
}

function sbDetectFromOrder(order) {
  const items = Array.isArray(order.line_items) ? order.line_items : [];
  const __ORD = `[ORDER ${order.id} ${order.name || ''}]`;
  if (!items.length) return null;

  const subBundleParents = (Array.isArray(order.line_items) ? order.line_items : []).filter(li => {
    const label = `${li.sku || ""} ${li.title || ""}`.toLowerCase();
    const isBundle = /bundle|pack/.test(label);
    const hasChildMarkers = !!bundleKey(li) || hasParentFlag(li) || anyAfterSellKey(li);
    const hasSub =
      String(order.tags || "").toLowerCase().includes("subscription") ||
      !!li.selling_plan_id ||
      !!(li.selling_plan_allocation && li.selling_plan_allocation.selling_plan_id);
    return isBundle && hasSub && !hasChildMarkers;
  });

  for (const li of items) {
    const props = Array.isArray(li.properties) ? li.properties : [];
    if (!props.length) continue;
    const prop = props.find(p => {
      const n = String(p.name || "").toLowerCase();
      return n.includes("aftersell") || n.includes("upcart");
    });
    if (!prop) continue;
    try {
      const raw = prop.value;
      if (typeof raw !== "string" || !raw.includes("{")) continue;
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed) && parsed.length) {
        const children = parsed.map(c => ({
          id: c.variant_id || c.id || Math.random(),
          title: c.title || "",
          sku: c.sku || "",
          quantity: c.qty || c.quantity || 1,
          price: toNum(c.price || 0),
          variant_id: c.variant_id || c.id || null
        }));
        console.log(__ORD, `AfterSell/UpCart bundle detected: ${children.length} children`);
        return { children, subBundleParents };
      }
    } catch (e) {
      console.log(__ORD, "AfterSell/UpCart parse error:", e.message);
    }
  }

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

  if ((zeroChildren.length && nonZero.length >= 1) || (zeroChildren.length && tagStr.includes("simple bundles"))) {
    if (subBundleParents.length) {
      for (const li of subBundleParents) {
        console.log(__ORD, "SUB-BUNDLE NO CHILDREN", { id: li.id, title: li.title, sku: li.sku, variant_id: li.variant_id });
      }
    }
    const usedIdx = new Set();
    for (const ch of zeroChildren) {
      const das = Array.isArray(ch.discount_allocations) ? ch.discount_allocations : [];
      for (const d of das) {
        if (typeof d.discount_application_index === "number") {
          usedIdx.add(d.discount_application_index);
        }
      }
    }
    const apps = Array.isArray(order.discount_applications) ? order.discount_applications : [];
    const bundleTitles = [];
    for (const i of usedIdx) {
      const app = apps[i];
      if (!app || !app.title) continue;
      const t = String(app.title);
      const m = t.match(/Simple Bundles:\s*([^|]+)/i);
      const name = (m ? m[1] : t).trim();
      if (name) bundleTitles.push(name.toLowerCase());
    }
    return { children: zeroChildren, subBundleParents, bundleTitles };
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
    const hasBundleKeyForAll = arr.every(x => !!bundleKey(x));
    if (hasBundleKeyForAll && arr.length >= 2) {
      arr.forEach(li => children.add(li));
      continue;
    }
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
    return { children: Array.from(children), subBundleParents };
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

async function isInLast10Orders(orderId) {
  try {
    const shop = process.env.SHOPIFY_SHOP;
    const token = process.env.SHOPIFY_ADMIN_ACCESS_TOKEN;
    if (!shop || !token) return true;
    const res = await fetch(`https://${shop}/admin/api/2025-01/orders.json?limit=150&status=any&order=created_at%20desc&fields=id`, {
      headers: { "X-Shopify-Access-Token": token }
    });
    if (!res.ok) return true;
    const data = await res.json();
    const ids = (data.orders || []).map(o => String(o.id));
    return ids.includes(String(orderId));
  } catch (_) {
    return true;
  }
}

function isMWOrder(order, conv){
  try {
    const items = Array.isArray(order?.line_items) ? order.line_items : [];
    const origCount = items.length;
    if (conv && Array.isArray(conv.after) && conv.after.length !== origCount) return true;
    const tags = String(order?.tags || "").toLowerCase();
    if (tags.includes("subscription")) return true;
    if (tags.includes("simple bundles")) return true;
    if (tags.includes("aftersell")) return true;
    if (tags.includes("upcart")) return true;
    if (tags.includes("bundle")) return true;
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

function mapSSStatus(o) {
  try {
    if (o.cancelled_at || (o.cancel_reason && String(o.cancel_reason).length)) return "cancelled";
    const tags = String(o.tags || "").toLowerCase();
    if (tags.includes("on hold") || tags.includes("hold")) return "on_hold";
    const fs = String(o.financial_status || "").toLowerCase();
    if (fs && fs !== "paid" && fs !== "partially_paid" && fs !== "partially_refunded" && fs !== "refunded") {
      return "awaiting_payment";
    }
    const ff = String(o.fulfillment_status || "").toLowerCase();
    if (ff === "fulfilled" || ff === "shipped") return "shipped";
    return "awaiting_shipment";
  } catch (_) {
    return "awaiting_shipment";
  }
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
    if (!imageUrl && variant.product_id) {
      const prodRes = await fetch(
        `https://${shop}/admin/api/2025-01/products/${variant.product_id}.json?fields=images`,
        { headers: { "X-Shopify-Access-Token": token } }
      );
      if (prodRes.ok) {
        const p = await prodRes.json();
        imageUrl = p.product.images?.[0]?.src || "";
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

async function getVariantBasics(variantId) {
  if (!variantId) return { title: `Variant ${variantId}`, sku: null, price: 0 };
  const shop = process.env.SHOPIFY_SHOP;
  const token = process.env.SHOPIFY_ADMIN_ACCESS_TOKEN;
  try {
    const res = await fetch(`https://${shop}/admin/api/2025-01/variants/${variantId}.json`, {
      headers: { "X-Shopify-Access-Token": token }
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const { variant } = await res.json();
    let vTitle = variant?.title || `Variant ${variantId}`;
    let vSKU = (variant?.sku || "").trim() || null;
    let vPrice = toNum(variant?.price);
    if ((vTitle || "").toLowerCase() === "default title" && variant?.product_id) {
      try {
        const pr = await fetch(`https://${shop}/admin/api/2025-01/products/${variant.product_id}.json?fields=title`, {
          headers: { "X-Shopify-Access-Token": token }
        });
        if (pr.ok) {
          const pj = await pr.json();
          if (pj?.product?.title) vTitle = pj.product.title;
        }
      } catch (_) {}
    }
    return { title: vTitle, sku: vSKU, price: vPrice };
  } catch {
    return { title: `Variant ${variantId}`, sku: null, price: 0 };
  }
}

async function transformOrder(order) {
  const after = [];
  const handled = new Set();
  const tags = String(order.tags || "").toLowerCase();
  const __ORD = `[ORDER ${order.id} ${order.name || ""}]`;
  const hasBundleTag =
    tags.includes("simple bundles") ||
    tags.includes("bundle") ||
    tags.includes("upcart") ||
    tags.includes("skio") ||
    tags.includes("subscription") ||
    tags.includes("aftersell");

  const sb = sbDetectFromOrder(order);

  if (!sb && !hasBundleTag) {
    console.log(__ORD, "SKIP ORDER (no bundle detected)");
    return { after: [] };
  }

  if (sb) {
    const HAS_CHILDREN = Array.isArray(sb.children) && sb.children.length > 0;
    if (HAS_CHILDREN) {
      for (const c of sb.children) {
        handled.add(c.id);
        const imageUrl = await getVariantImage(c.variant_id);
        pushLine(after, c, imageUrl);
        console.log(__ORD, "SB CHILD", {
          id: c.id,
          title: c.title,
          variant_id: c.variant_id,
          imageUrl: imageUrl ? "OK" : "NO"
        });
      }
      const isLikelyBundleParent = (li) =>
        hasParentFlag(li) ||
        bundleKey(li) ||
        (anyAfterSellKey(li) && looksLikeBundle(li));
      for (const li of (order.line_items || [])) {
        if (isLikelyBundleParent(li)) {
          handled.add(li.id);
        }
      }
      if (Array.isArray(sb.bundleTitles) && sb.bundleTitles.length) {
        const namesSet = new Set(sb.bundleTitles.map(s => s.toLowerCase()));
        for (const li of (order.line_items || [])) {
          const titleNorm = String(li.title || "").toLowerCase();
          if (namesSet.has(titleNorm)) {
            handled.add(li.id);
          }
        }
      }
    }
    const subParents = HAS_CHILDREN ? [] : (Array.isArray(sb.subBundleParents) ? sb.subBundleParents : []);
    for (const li of subParents) {
      try {
        const { buildBundleMap } = await import("./scanner_runtime.js");
        let map = await buildBundleMap({ onlyProductId: String(li.product_id) });
        let kids = map[`product:${li.product_id}`] || [];
        if (!kids.length) {
          map = await buildBundleMap({ onlyVariantId: String(li.variant_id) });
          kids = map[`variant:${li.variant_id}`] || [];
          if (!kids.length) {
            const vKeys = Object.keys(map).filter(k => k.startsWith("variant:"));
            if (vKeys.length === 1) kids = map[vKeys[0]] || [];
          }
        }
        if (kids.length) {
          console.log(__ORD, "SCANNER EXPAND SUB PARENT", {
            li_id: li.id,
            product_id: li.product_id,
            variant_id: li.variant_id,
            found_children: kids.length
          });
          for (const ch of kids) {
            const vid = String(ch.variantId || ch.variant_id || ch.id || "").trim();
            const qty = Math.max(1, Number(ch.qty || ch.quantity || 1));
            if (!vid) continue;
            const vb = await getVariantBasics(vid);
            const imageUrl = await getVariantImage(vid);
            after.push({
              id: `${li.id}:${vid}`,
              title: vb.title,
              sku: vb.sku,
              qty: qty * (li.quantity || 1),
              unitPrice: vb.price,
              imageUrl
            });
          }
          handled.add(li.id);
        } else {
          console.log(__ORD, "SCANNER EXPAND SUB PARENT — NO KIDS — OUTPUT PARENT");
          const imageUrl = await getVariantImage(li.variant_id);
          pushLine(after, li, imageUrl);
          handled.add(li.id);
        }
      } catch (e) {
        console.log(__ORD, "SCANNER EXPAND ERROR", String(e && e.message || e), "→ OUTPUT PARENT");
        const imageUrl = await getVariantImage(li.variant_id);
        pushLine(after, li, imageUrl);
        handled.add(li.id);
      }
    }
  }

  const ubCandidates = [];
  for (const li of (order.line_items || [])) {
    const props = Array.isArray(li.properties) ? li.properties : [];
    const hasUpcartSub = props.some(p => String(p?.name || "").toLowerCase().includes("__upcartsubscriptionupgrade"));
    const hasPlan = !!(li.selling_plan_id || li.selling_plan_allocation?.selling_plan_id);
    const isBundleSku = String(li.sku || "").toLowerCase().includes("bundle");
    const isBundleTitle = String(li.title || "").toLowerCase().includes("bundle");
    if ((hasUpcartSub || hasPlan) && (isBundleSku || isBundleTitle)) ubCandidates.push(li);
  }

  for (const li of ubCandidates) {
    if (handled.has(li.id)) continue;
    try {
      const { buildBundleMap } = await import("./scanner_runtime.js");
      let map = await buildBundleMap({ onlyVariantId: String(li.variant_id) });
      let recipe = map[`variant:${li.variant_id}`];
      if (!Array.isArray(recipe) || !recipe.length) {
        map = await buildBundleMap({ onlyProductId: String(li.product_id) });
        recipe = map[`product:${li.product_id}`];
        if (!recipe || !recipe.length) {
          const vKeys = Object.keys(map).filter(k => k.startsWith("variant:"));
          if (vKeys.length === 1) recipe = map[vKeys[0]] || [];
        }
      }
      if (Array.isArray(recipe) && recipe.length) {
        console.log(__ORD, "SCANNER EXPAND UB PARENT", {
          li_id: li.id,
          product_id: li.product_id,
          variant_id: li.variant_id,
          found_children: recipe.length
        });
        for (const r of recipe) {
          const vid = String(r.variantId || r.variant_id || r.id || "").trim();
          const qty = Math.max(1, Number(r.qty || r.quantity || 1));
          if (!vid) continue;
          const vb = await getVariantBasics(vid);
          const imageUrl = await getVariantImage(vid);
          after.push({
            id: `${li.id}:${vid}`,
            title: vb.title,
            sku: vb.sku,
            qty: qty * (li.quantity || 1),
            unitPrice: vb.price,
            imageUrl
          });
        }
        handled.add(li.id);
      } else {
        console.log(__ORD, "SCANNER EXPAND UB PARENT — NO KIDS — OUTPUT PARENT");
        const imageUrl = await getVariantImage(li.variant_id);
        pushLine(after, li, imageUrl);
        handled.add(li.id);
      }
    } catch (e) {
      console.log(__ORD, "SCANNER EXPAND ERROR", String(e && e.message || e), "→ OUTPUT PARENT");
      const imageUrl = await getVariantImage(li.variant_id);
      pushLine(after, li, imageUrl);
      handled.add(li.id);
    }
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
    let order = JSON.parse(raw.toString("utf8"));
    const topic = String(req.headers["x-shopify-topic"] || "");
async function refetchOrderById(id){
  const shop = process.env.SHOPIFY_SHOP;
  const admin = process.env.SHOPIFY_ADMIN_ACCESS_TOKEN;
  if (!shop || !admin) return null;
  const r = await fetch(`https://${shop}/admin/api/2025-01/orders/${encodeURIComponent(id)}.json`, { headers: { "X-Shopify-Access-Token": admin } });
  if (!r.ok) return null;
  const j = await r.json();
  return j?.order || null;
}
async function refetchWithRetry(id, tries, delayMs){
  let o = null;
  for (let i=0;i<tries;i++){
    o = await refetchOrderById(id);
    const tags = String(o?.tags || "").toLowerCase();
    const hasTag = /(^|,)\s*(simple bundles|bundle|upcart|skio|subscription|aftersell)\s*(,|$)/.test(`,${tags},`);
    const hasPlan = Array.isArray(o?.line_items) && o.line_items.some(li => li?.selling_plan_id || li?.selling_plan_allocation?.selling_plan_id);
    const hasSbProps = Array.isArray(o?.line_items) && o.line_items.some(li => Array.isArray(li.properties) && li.properties.some(p => String(p?.name||"").toLowerCase().includes("_sb_")));
    if (hasTag || hasPlan || hasSbProps) return o;
    await new Promise(r => setTimeout(r, delayMs));
  }
  return o;
}

    const okRecent = await isInLast10Orders(order.id);
    if (!okRecent) {
      console.log("SKIP OLD ORDER (not in last 10):", order.id, order.name);
      res.status(200).send("ok");
      return;
    }
if (topic === "orders/create") {
  const o2 = await refetchWithRetry(order.id, 3, 1500);
  if (o2) order = o2;
} else {
  const tags = String(order.tags || "").toLowerCase();
  const hasTag = /(^|,)\s*(simple bundles|bundle|upcart|skio|subscription|aftersell)\s*(,|$)/.test(`,${tags},`);
  const hasPlan = Array.isArray(order.line_items) && order.line_items.some(li => li?.selling_plan_id || li?.selling_plan_allocation?.selling_plan_id);
  const hasSbProps = Array.isArray(order.line_items) && order.line_items.some(li => Array.isArray(li.properties) && li.properties.some(p => String(p?.name||"").toLowerCase().includes("_sb_")));
  if (!(hasTag || hasPlan || hasSbProps)) {
    const o2 = await refetchWithRetry(order.id, 2, 1200);
    if (o2) order = o2;
  }
}


if (!isMWOrder(order, null)) {
  console.log("[ORDER", order.id, order.name || "", "] SKIP: non-MW (no tags/flags)");
  res.status(200).send("ok");
  return;
}

    const conv = await transformOrder(order);
    remember({
      id: order.id,
      name: order.name,
      email: order.email || "",
      shipping_address: order.shipping_address || {},
      billing_address: order.billing_address || {},
      payload: conv,
      created_at: order.created_at || new Date().toISOString(),
      _ss_status: mapSSStatus(order)
    });
    statusById.set(order.id, "awaiting_shipment");
    if (isMWOrder(order, conv)) {
      scheduleSSRefresh();
    }
    console.log("ORDER PROCESSED", order.id, "items:", conv.after.length);
    res.status(200).send("ok");
  } catch (e) {
    console.error("Webhook error:", e);
    res.status(500).send("error");
  }
}

app.post("/admin/ss-hook", express.json(), async (req, res) => {
  try {
    const sec = req.headers["x-ss-webhook-secret"] || "";
    const okSecret =
      (process.env.SS_WH_SECRET && sec === process.env.SS_WH_SECRET) ||
      (process.env.ADMIN_KEY && sec === process.env.ADMIN_KEY) ||
      (process.env.SS_PASS && sec === process.env.SS_PASS);
    if (!okSecret) {
      res.status(401).json({ ok: false, error: "bad secret" });
      return;
    }

    const b = req.body || {};
    const rt = String(b.resource_type || "").toUpperCase();
    const allow = ["SHIP_NOTIFY", "SHIPMENT_NOTIFY", "SHIPMENT_SHIPPED", "ORDER_SHIPPED"];
    if (!allow.includes(rt)) {
      res.status(200).json({ ok: true, action: "noop_unsupported_type", type: rt });
      return;
    }

    const orderNumber = String(b.orderNumber || b.order_number || "").trim();
    const trackingNumber = String(b.trackingNumber || b.tracking || b.tracking_number || "").trim();
    const carrierCode = String(b.carrierCode || b.carrier || "").trim();
    const trackingUrl = String(b.trackingUrl || b.tracking_url || "").trim();

    if (!orderNumber || !trackingNumber) {
      res.status(400).json({ ok: false, error: "missing orderNumber or trackingNumber" });
      return;
    }

    const so = await shopifyFindOrderByNumber(orderNumber);
    if (!so) {
      res.status(404).json({ ok: false, error: "shopify order not found", orderNumber });
      return;
    }

    const shop = process.env.SHOPIFY_SHOP;
    const token = process.env.SHOPIFY_ADMIN_ACCESS_TOKEN;

    const ffsRes = await fetch(`https://${shop}/admin/api/2025-01/orders/${so.id}/fulfillments.json`, {
      headers: { "X-Shopify-Access-Token": token }
    });
    if (!ffsRes.ok) {
      const t = await ffsRes.text().catch(()=>"");
      res.status(502).json({ ok: false, error: `fulfillments fetch ${ffsRes.status}`, body: t.slice(0,400) });
      return;
    }
    const ffsJson = await ffsRes.json();
    const fulfillments = Array.isArray(ffsJson.fulfillments) ? ffsJson.fulfillments : [];
    const active = fulfillments.filter(f => String(f.status).toLowerCase() !== "cancelled");
    const lastFf = active.sort((a,b)=>new Date(a.created_at)-new Date(b.created_at)).slice(-1)[0];

    const unfulfilled = (Array.isArray(so.line_items) ? so.line_items : [])
      .filter(li => Number(li.fulfillable_quantity || 0) > 0)
      .map(li => ({ id: li.id, quantity: li.fulfillable_quantity }));

    if (!lastFf && unfulfilled.length > 0) {
      const bodyCreate = shopifyBuildFulfillmentBody(so, {
        carrierCode,
        carrier: carrierCode,
        trackingNumber,
        trackingUrl
      });
      bodyCreate.fulfillment.line_items = unfulfilled;
      await shopifyCreateFulfillment(so.id, bodyCreate);
      res.status(200).json({ ok: true, action: "created_fulfillment", orderId: so.id, orderNumber, trackingNumber });
      return;
    }

    const ffId = lastFf ? lastFf.id : null;
    if (!ffId) {
      res.status(200).json({ ok: true, action: "noop_no_open_and_no_ff", orderId: so.id });
      return;
    }

    const updPayload = {
      fulfillment: {
        notify_customer: false,
        tracking_info: {
          number: trackingNumber,
          url: trackingUrl || null,
          company: carrierCode || null
        }
      }
    };

    const updRes = await fetch(`https://${shop}/admin/api/2025-01/fulfillments/${ffId}/update_tracking.json`, {
      method: "POST",
      headers: { "X-Shopify-Access-Token": token, "Content-Type": "application/json" },
      body: JSON.stringify(updPayload)
    });
    if (!updRes.ok) {
      const t = await updRes.text().catch(()=>"");
      res.status(502).json({ ok: false, error: `update_tracking ${updRes.status}`, body: t.slice(0,400) });
      return;
    }

    res.status(200).json({ ok: true, action: "updated_tracking", orderId: so.id, fulfillmentId: ffId, trackingNumber });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e.message || e) });
  }
});

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
  const shipStationStatus = o._ss_status || "awaiting_shipment";
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

function minimalOrderNode(o) {
  const items = (o?.payload?.after || []).map(i => ({
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
  const shipStationStatus = o._ss_status || "awaiting_shipment";
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
  return `
  <Order>
    <OrderID>${esc(o.id)}</OrderID>
    <OrderNumber>${esc((o.name || "").replace(/^#/, ''))}</OrderNumber>
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
  </Order>`;
}

function xmlForMany(orders) {
  if (!Array.isArray(orders) || orders.length === 0) {
    return `<?xml version="1.0" encoding="utf-8"?><Orders></Orders>`;
  }
  const body = orders.map(minimalOrderNode).join("\n");
  return `<?xml version="1.0" encoding="utf-8"?>\n<Orders>\n${body}\n</Orders>`;
}

function authOK(req) {
  const h = req.headers.authorization || "";
  if (h.startsWith("Basic ")) {
    const [u, p] = Buffer.from(h.slice(6), "base64").toString("utf8").split(":", 2);
    return u === process.env.SS_USER && p === process.env.SS_PASS;
  }
  return false;
}

app.use("/shipstation", async (req, res) => {
  res.set("Content-Type", "application/xml; charset=utf-8");
res.set("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0");
res.set("Pragma", "no-cache");
res.set("Expires", "0");
res.set("Surrogate-Control", "no-store");
res.set("Vary", "since, ids");

  const checkId = req.query.checkorder_id;
  if (checkId) {
    const shop = process.env.SHOPIFY_SHOP;
    const admin = process.env.SHOPIFY_ADMIN_ACCESS_TOKEN;
    if (!shop || !admin) {
      res.status(500).send(`<?xml version="1.0" encoding="utf-8"?><Error>Shopify creds missing</Error>`);
      return;
    }
    try {
      const r = await fetch(`https://${shop}/admin/api/2025-01/orders/${encodeURIComponent(checkId)}.json`, {
        headers: { "X-Shopify-Access-Token": admin }
      });
      if (!r.ok) {
        const body = await r.text().catch(()=>"");
        res.status(502).send(`<?xml version="1.0" encoding="utf-8"?><Error>Shopify fetch failed ${r.status} ${r.statusText} ${esc(body)}</Error>`);
        return;
      }
      const data = await r.json();
      const order = data.order || data;
      const conv = await transformOrder(order);
      const shadow = {
        id: order.id,
        name: order.name,
        email: order.email || "",
        shipping_address: order.shipping_address || {},
        billing_address: order.billing_address || {},
        payload: conv,
        created_at: order.created_at || new Date().toISOString(),
        _ss_status: mapSSStatus(order)
      };
      
      res.status(200).send(xmlForMany([shadow]));
      return;
    } catch (e) {
      res.status(500).send(`<?xml version="1.0" encoding="utf-8"?><Error>Reprocess error</Error>`);
      return;
    }
  }

  if (!process.env.SS_USER || !process.env.SS_PASS || !authOK(req)) {
    res.status(401).send(`<?xml version="1.0" encoding="utf-8"?><Error>Authentication failed</Error>`);
    return;
  }

  const limit = Math.max(1, Math.min(100, Number(req.query.limit || 25)));
  const sinceStr = String(req.query.since || "").trim();
  const ids = String(req.query.ids || "").trim();

  let candidates = [];

  if (ids) {
    const list = ids.split(",").map(s => s.trim()).filter(Boolean);
    for (const id of list) {
      const o = bestById.get(String(id));
      if (o) candidates.push(o);
    }
  } else {
    candidates = Array.from(bestById.values());
  }

  candidates.sort((a,b) => {
    const ta = Date.parse(a?.created_at || "") || 0;
    const tb = Date.parse(b?.created_at || "") || 0;
    return tb - ta;
  });

  if (sinceStr) {
    const ts = Date.parse(sinceStr);
    if (Number.isFinite(ts)) {
      candidates = candidates.filter(o => {
        const t = Date.parse(o?.created_at || "") || 0;
        return t >= ts;
      });
    }
  }

  const picked = candidates.slice(0, limit);
  res.send(xmlForMany(picked));
});

app.get("/health", async (req, res) => {
  try {
    if (req.query && req.query.dump === "1" && req.query.product_id) {
      const { dumpMeta } = await import("./scanner_runtime.js");
      const pid = String(req.query.product_id);
      const data = await dumpMeta(pid);
      res.set("Content-Type", "application/json; charset=utf-8");
      res.status(200).send(JSON.stringify(data, null, 2));
      return;
    }
    if (req.query && (req.query.scan === "1" || req.query.scan_bundle_map === "1")) {
      const { buildBundleMap } = await import("./scanner_runtime.js");
      const productId = req.query.product_id ? String(req.query.product_id) : null;
      const variantId = req.query.variant_id ? String(req.query.variant_id) : null;
      const map = await buildBundleMap({ onlyProductId: productId, onlyVariantId: variantId });
      res.set("Content-Type", "application/json; charset=utf-8");
      res.set("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0");
res.set("Pragma", "no-cache");
res.set("Expires", "0");
res.set("Surrogate-Control", "no-store");
res.set("Vary", "since, ids");

      res.status(200).send(JSON.stringify({ ok: true, keys: Object.keys(map).length, map }, null, 2));
      return;
    }
    res.send("ok");
  } catch (e) {
    res.status(500).send({ ok: false, error: String(e.message || e) });
  }
});

function authAdmin(req) {
  const key = req.headers["x-admin-key"] || "";
  return key && process.env.ADMIN_KEY && key === process.env.ADMIN_KEY;
}

async function ssFetchShipments({ since, page = 1, pageSize = 100, orderNumbers = [] }, token) {
  const hdrs = { "API-Key": token };
  const startIso = since ? new Date(`${since}T00:00:00Z`).toISOString() : null;
  const endIso = new Date().toISOString();
  const u = new URL("https://api.shipstation.com/v2/shipments");
  if (startIso) {
    u.searchParams.set("modified_at_start", startIso);
    u.searchParams.set("modified_at_end", endIso);
    u.searchParams.set("sort_by", "modified_at");
    u.searchParams.set("sort_dir", "desc");
  } else {
    u.searchParams.set("sort_by", "created_at");
    u.searchParams.set("sort_dir", "desc");
  }
  u.searchParams.set("page", String(page));
  u.searchParams.set("page_size", String(Math.min(500, Math.max(1, pageSize))));
  const res = await fetch(u.toString(), { headers: hdrs });
  if (!res.ok) throw new Error(`SS ${res.status}`);
  const data = await res.json();
  let shipments = Array.isArray(data.shipments) ? data.shipments : [];
  shipments = shipments.filter(s => {
    const on = s.orderNumber || s.order_number;
    const tn = s.trackingNumber || s.tracking_number;
    return on && tn;
  });
  if (orderNumbers.length) {
    const set = new Set(orderNumbers.map(String));
    shipments = shipments.filter(s => set.has(String(s.orderNumber || s.order_number)));
  }
  return { shipments, pages: data.pages || 1 };
}


async function shopifyCreateFulfillment(orderId, body) {
  const shop = process.env.SHOPIFY_SHOP;
  const token = process.env.SHOPIFY_ADMIN_ACCESS_TOKEN;
  const url = `https://${shop}/admin/api/2025-01/orders/${orderId}/fulfillments.json`;
  const r = await fetch(url, {
    method: "POST",
    headers: { "X-Shopify-Access-Token": token, "Content-Type": "application/json" },
    body: JSON.stringify(body)
  });
  if (!r.ok) {
    const t = await r.text().catch(()=>"");
    throw new Error(`Shopify fulfill err ${r.status} ${t}`);
  }
  return r.json();
}

app.post("/admin/backfill-shipments", express.json(), async (req, res) => {
  try {
    if (!authAdmin(req)) {
      res.status(401).json({ ok: false, error: "unauthorized" });
      return;
    }
    const token = req.header("API-Key") || process.env.SS_TOKEN || process.env.SS_V2_TOKEN || "";
    if (!token) {
      res.status(400).json({ ok: false, error: "missing ShipStation API-Key" });
      return;
    }
    const since = (req.query.since || req.body?.since || "").toString().trim();
    const orderNumbers = Array.isArray(req.body?.orderNumbers) ? req.body.orderNumbers.map(String) : [];
    if (!since && !orderNumbers.length) {
      res.status(400).json({ ok: false, error: "provide ?since=YYYY-MM-DD or body.orderNumbers[]" });
      return;
    }

    const results = [];
    let page = 1, pages = 1;

    do {
      const { shipments, pages: totalPages } = await ssFetchShipments({ since, page, pageSize: 200, orderNumbers }, token);
      pages = totalPages || 1;

      for (const s of shipments) {
        try {
          const orderNumber = String(s.orderNumber || s.order_number || "").trim();
          const trackingNumber = String(s.trackingNumber || s.tracking_number || "").trim();
          const carrierCode = String(s.carrierCode || s.carrier_code || "").trim();
          const trackingUrl = String(s.trackingUrl || s.tracking_url || "").trim();

          if (!orderNumber || !trackingNumber) {
            results.push({ shipment_id: s.shipment_id || null, status: "skip_no_order_or_tracking" });
            continue;
          }

          const so = await shopifyFindOrderByNumber(orderNumber);
          if (!so) {
            results.push({ orderNumber, status: "skip_no_shopify_order" });
            continue;
          }

          const ff = String(so.fulfillment_status || "").toLowerCase();
          const allFulfilled = ff === "fulfilled" || ff === "shipped";
          const unfulfilledLeft = (Array.isArray(so.line_items) ? so.line_items : []).some(li => Number(li.fulfillable_quantity || 0) > 0);

          if (!unfulfilledLeft || allFulfilled) {
            results.push({ orderId: so.id, orderNumber, status: "already_fulfilled" });
            continue;
          }

          const body = shopifyBuildFulfillmentBody(so, {
            carrierCode,
            carrier: carrierCode,
            trackingNumber,
            trackingUrl
          });

          await shopifyCreateFulfillment(so.id, body);
          results.push({ orderId: so.id, orderNumber, status: "fulfilled", tracking: trackingNumber });
        } catch (e) {
          results.push({ status: "error", error: String(e.message || e) });
        }
      }

      page += 1;
    } while (page <= pages);

    res.json({ ok: true, since: since || null, count: results.length, results });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e.message || e) });
  }
});


app.get("/admin/backfill-shipments", async (req, res) => {
  try {
    if (!authAdmin(req)) {
      res.status(401).json({ ok: false, error: "unauthorized" });
      return;
    }
    const since = (req.query.since || "").toString().trim();
    const ordersParam = (req.query.orders || "").toString().trim();
    const orderNumbers = ordersParam ? ordersParam.split(",").map(s => s.trim()).filter(Boolean) : [];
    if (!since && !orderNumbers.length) {
      res.status(400).json({ ok: false, error: "provide ?since=YYYY-MM-DD or ?orders=comma-separated" });
      return;
    }

    const results = [];
    let page = 1, pages = 1;

    do {
      const { shipments, pages: totalPages } = await ssFetchShipments({ since, page, pageSize: 200, orderNumbers });
      pages = totalPages || 1;

      for (const s of shipments) {
        try {
          const so = await shopifyFindOrderByNumber(s.orderNumber);
          if (!so) {
            results.push({ orderNumber: s.orderNumber, status: "skip_no_shopify_order" });
            continue;
          }

          const ff = String(so.fulfillment_status || "").toLowerCase();
          const allFulfilled = ff === "fulfilled" || ff === "shipped";
          const unfulfilledLeft = (Array.isArray(so.line_items) ? so.line_items : []).some(li => Number(li.fulfillable_quantity || 0) > 0);

          if (!unfulfilledLeft || allFulfilled) {
            results.push({ orderId: so.id, orderNumber: s.orderNumber, status: "already_fulfilled" });
            continue;
          }

          const body = shopifyBuildFulfillmentBody(so, {
            carrierCode: s.carrierCode,
            carrier: s.carrierCode,
            trackingNumber: s.trackingNumber,
            trackingUrl: s.trackingUrl
          });

          await shopifyCreateFulfillment(so.id, body);
          results.push({ orderId: so.id, orderNumber: s.orderNumber, status: "fulfilled", tracking: s.trackingNumber });
        } catch (e) {
          results.push({ orderNumber: s.orderNumber, status: "error", error: String(e.message || e) });
        }
      }

      page += 1;
    } while (page <= pages);

    res.json({ ok: true, since: since || null, count: results.length, results });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e.message || e) });
  }
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
