// scanner_runtime.js (ESM)

const SHOP = process.env.SHOPIFY_SHOP;
const TOKEN = process.env.SHOPIFY_ADMIN_ACCESS_TOKEN;

const SB_NS_CANDIDATES = [
  { ns: "simple_bundles_2_0", key: "components" },
  { ns: "simple_bundles", key: "components" },
  { ns: "simplebundles", key: "components" },
  { ns: "bundles", key: "components" }
];

function toNum(x) {
  const n = Number(x);
  return Number.isFinite(n) ? n : 0;
}

async function adminFetch(path) {
  const url = `https://${SHOP}/admin/api/2025-01${path}`;
  const r = await fetch(url, {
    headers: { "X-Shopify-Access-Token": TOKEN, "Content-Type": "application/json" }
  });
  if (!r.ok) {
    const t = await r.text().catch(() => "");
    throw new Error(`HTTP ${r.status} ${r.statusText} ${t}`);
  }
  return r;
}

async function listAllProducts() {
  const out = [];
  let since = 0;
  for (;;) {
    const r = await adminFetch(`/products.json?fields=id&limit=250${since ? `&since_id=${since}` : ""}`);
    const j = await r.json();
    const arr = Array.isArray(j.products) ? j.products : [];
    if (!arr.length) break;
    for (const p of arr) out.push({ id: p.id });
    since = arr[arr.length - 1].id;
    if (arr.length < 250) break;
  }
  return out;
}

async function listProductVariants(productId) {
  const r = await adminFetch(`/products/${productId}.json?fields=variants`);
  const j = await r.json();
  const v = Array.isArray(j.product?.variants) ? j.product.variants : [];
  return v.map(x => ({ id: x.id }));
}

async function listProductMetafields(productId) {
  let out = [];
  let page = null;
  for (;;) {
    const r = await adminFetch(`/products/${productId}/metafields.json?limit=250${page ? `&page_info=${encodeURIComponent(page)}` : ""}`);
    const j = await r.json();
    const arr = Array.isArray(j.metafields) ? j.metafields : [];
    out = out.concat(arr.map(m => ({ namespace: m.namespace, key: m.key, value: m.value, type: m.type })));
    const link = r.headers.get("link") || "";
    const m = link.match(/<[^>]*page_info=([^&>]+)[^>]*>; rel="next"/i);
    if (m) { page = m[1]; continue; }
    break;
  }
  return out;
}

async function listVariantMetafields(variantId) {
  let out = [];
  let page = null;
  for (;;) {
    const r = await adminFetch(`/variants/${variantId}/metafields.json?limit=250${page ? `&page_info=${encodeURIComponent(page)}` : ""}`);
    const j = await r.json();
    const arr = Array.isArray(j.metafields) ? j.metafields : [];
    out = out.concat(arr.map(m => ({ namespace: m.namespace, key: m.key, value: m.value, type: m.type })));
    const link = r.headers.get("link") || "";
    const m = link.match(/<[^>]*page_info=([^&>]+)[^>]*>; rel="next"/i);
    if (m) { page = m[1]; continue; }
    break;
  }
  return out;
}

function validateVariantId(x, knownVariantIds = null) {
  const s = String(x || "").trim();
  if (!/^\d{6,}$/.test(s)) return null;
  if (knownVariantIds && !knownVariantIds.has(s)) return null;
  return s;
}

function normalizeRecipe(list, knownVariantIds = null) {
  const out = [];
  for (const it of list || []) {
    const vid = validateVariantId(it.variantId, knownVariantIds);
    const qty = Math.max(1, Number(it.qty || 1));
    if (vid) out.push({ variantId: vid, qty });
  }
  return out;
}

function parseRecipeFromValue(value, type, knownVariantIds = null) {
  if (String(type || "").includes("product_variant_reference")) {
    try {
      const parsed = JSON.parse(String(value));
      const arr = Array.isArray(parsed) ? parsed : [parsed];
      const tmp = [];
      for (const it of arr) {
        const raw = it?.id ?? it;
        const m = String(raw || "").match(/ProductVariant\/(\d+)/);
        const vid = m ? m[1] : null;
        if (vid) tmp.push({ variantId: vid, qty: 1 });
      }
      return normalizeRecipe(tmp, knownVariantIds);
    } catch (_) {
      const parts = String(value || "").split(/[\s,;|]+/).map(s => s.trim()).filter(Boolean);
      const tmp = [];
      for (const p of parts) {
        const m = p.match(/ProductVariant\/(\d+)/) || p.match(/^(\d{6,})$/);
        if (m) tmp.push({ variantId: m[1], qty: 1 });
      }
      return normalizeRecipe(tmp, knownVariantIds);
    }
  }
  try {
    const parsed = JSON.parse(String(value));
    const arr = Array.isArray(parsed) ? parsed : (Array.isArray(parsed?.components) ? parsed.components : null);
    if (arr && arr.length) {
      const tmp = [];
      for (const it of arr) {
        const raw = it?.variantId || it?.variant_id || (typeof it?.id === "string" && it.id.includes("ProductVariant/") ? it.id : it?.id);
        let vid = null;
        if (typeof raw === "string" && raw.includes("ProductVariant/")) {
          const m = raw.match(/ProductVariant\/(\d+)/);
          if (m) vid = m[1];
        } else if (/^\d{6,}$/.test(String(raw || ""))) {
          vid = String(raw);
        }
        const qty = Math.max(1, Number(it?.qty || it?.quantity || it?.qty_each || 1));
        if (vid) tmp.push({ variantId: vid, qty });
      }
      return normalizeRecipe(tmp, knownVariantIds);
    }
  } catch (_) {}
  const flat = String(value || "");
  if (flat) {
    const tmp = [];
    for (const token of flat.split(/[,;|]/)) {
      const m = token.trim().match(/(?:ProductVariant\/)?(\d{6,})\s*:\s*(\d+)/i);
      if (m) tmp.push({ variantId: m[1], qty: Number(m[2]) || 1 });
    }
    return normalizeRecipe(tmp, knownVariantIds);
  }
  return [];
}

function pickRecipeFromMetafields(mfs, knownVariantIds = null) {
  if (!Array.isArray(mfs) || !mfs.length) return [];
  for (const cand of SB_NS_CANDIDATES) {
    const mf = mfs.find(m => m.namespace === cand.ns && m.key === cand.key && m.value);
    if (mf) {
      const r = parseRecipeFromValue(mf.value, mf.type, knownVariantIds);
      if (r.length) return r;
    }
  }
  for (const mf of mfs) {
    if (!mf?.value) continue;
    const r = parseRecipeFromValue(mf.value, mf.type, knownVariantIds);
    if (r.length) return r;
  }
  return [];
}

export async function dumpMeta(productId) {
  const variants = await listProductVariants(productId);
  const knownIds = new Set(variants.map(v => String(v.id)));
  const product_metafields = await listProductMetafields(productId);
  const product_recipe = pickRecipeFromMetafields(product_metafields, knownIds);
  const variant_recipes = {};
  for (const v of variants) {
    const m = await listVariantMetafields(v.id);
    variant_recipes[v.id] = pickRecipeFromMetafields(m, knownIds);
  }
  return {
    ok: true,
    product_id: String(productId),
    product_recipe,
    variants: variants.map(v => ({ id: v.id })),
    variant_recipes,
    product_metafields,
    variant_metafields: undefined
  };
}

export async function buildBundleMap({ onlyProductId = null, onlyVariantId = null } = {}) {
  const out = {};
  if (onlyVariantId) {
    const pidRes = await adminFetch(`/variants/${onlyVariantId}.json?fields=product_id`);
    const pidJson = await pidRes.json();
    const pid = pidJson?.variant?.product_id;
    if (!pid) return out;
    const vars = await listProductVariants(pid);
    const knownIds = new Set(vars.map(v => String(v.id)));
    const mfs = await listVariantMetafields(onlyVariantId);
    const rec = pickRecipeFromMetafields(mfs, knownIds);
    if (rec.length) out[`variant:${onlyVariantId}`] = rec;
    return out;
  }
  if (onlyProductId) {
    const vars = await listProductVariants(onlyProductId);
    const knownIds = new Set(vars.map(v => String(v.id)));
    const pMfs = await listProductMetafields(onlyProductId);
    const pRec = pickRecipeFromMetafields(pMfs, knownIds);
    if (pRec.length) out[`product:${onlyProductId}`] = pRec;
    for (const v of vars) {
      const mfs = await listVariantMetafields(v.id);
      const r = pickRecipeFromMetafields(mfs, knownIds);
      if (r.length) out[`variant:${v.id}`] = r;
    }
    return out;
  }
  const okIds = [];
  const missIds = [];
  const products = await listAllProducts();
  for (const p of products) {
    const vars = await listProductVariants(p.id);
    const knownIds = new Set(vars.map(v => String(v.id)));
    let found = false;
    const pMfs = await listProductMetafields(p.id);
    const pRec = pickRecipeFromMetafields(pMfs, knownIds);
    if (pRec.length) { out[`product:${p.id}`] = pRec; found = true; }
    for (const v of vars) {
      const mfs = await listVariantMetafields(v.id);
      const r = pickRecipeFromMetafields(mfs, knownIds);
      if (r.length) { out[`variant:${v.id}`] = r; found = true; }
    }
    if (found) okIds.push(String(p.id)); else missIds.push(String(p.id));
  }
  out["__REPORT_OK"] = okIds;
  out["__REPORT_MISS"] = missIds;
  out["__stats"] = { total: products.length, ok: okIds.length, miss: missIds.length };
  return out;
}
