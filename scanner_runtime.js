// scanner_runtime.js
// Работает через REST Admin API. Не требует GraphQL схем HasMetafieldsIdentifier.
// Ничего не меняет в логике цен/ордеров — только сканирование рецептов бандлов.

const API_VERSION = "2025-01";

function envOrThrow(name) {
  const v = process.env[name];
  if (!v) throw new Error(`${name} is not set`);
  return v;
}
function toNum(x) {
  const n = Number(x);
  return Number.isFinite(n) ? n : 0;
}
async function fetchJSON(url, opts = {}) {
  const res = await fetch(url, opts);
  if (!res.ok) {
    const t = await res.text().catch(() => "");
    throw new Error(`${res.status} ${res.statusText} :: ${t}`);
  }
  return res.json();
}

// ---------- REST helpers ----------
async function getProduct(productId) {
  const shop = envOrThrow("SHOPIFY_SHOP");
  const token = envOrThrow("SHOPIFY_ADMIN_ACCESS_TOKEN");
  const url = `https://${shop}/admin/api/${API_VERSION}/products/${productId}.json`;
  const j = await fetchJSON(url, { headers: { "X-Shopify-Access-Token": token } });
  return j.product;
}
async function listProductMetafields(productId) {
  const shop = envOrThrow("SHOPIFY_SHOP");
  const token = envOrThrow("SHOPIFY_ADMIN_ACCESS_TOKEN");
  const url = `https://${shop}/admin/api/${API_VERSION}/products/${productId}/metafields.json`;
  const j = await fetchJSON(url, { headers: { "X-Shopify-Access-Token": token } });
  return Array.isArray(j.metafields) ? j.metafields : [];
}
async function listVariantMetafields(variantId) {
  const shop = envOrThrow("SHOPIFY_SHOP");
  const token = envOrThrow("SHOPIFY_ADMIN_ACCESS_TOKEN");
  const url = `https://${shop}/admin/api/${API_VERSION}/variants/${variantId}/metafields.json`;
  const j = await fetchJSON(url, { headers: { "X-Shopify-Access-Token": token } });
  return Array.isArray(j.metafields) ? j.metafields : [];
}
async function listProductVariants(productId) {
  const shop = envOrThrow("SHOPIFY_SHOP");
  const token = envOrThrow("SHOPIFY_ADMIN_ACCESS_TOKEN");
  const url = `https://${shop}/admin/api/${API_VERSION}/products/${productId}/variants.json?limit=250`;
  const j = await fetchJSON(url, { headers: { "X-Shopify-Access-Token": token } });
  return Array.isArray(j.variants) ? j.variants : [];
}

// ---------- parsing ----------
const SB_NS_CANDIDATES = [
  { ns: "simple_bundles_2_0", key: "components" },
  { ns: "simple_bundles",     key: "components" },
  { ns: "simplebundles",      key: "components" },
  { ns: "bundles",            key: "components" },
];

function parseGidVariantId(g) {
  if (!g) return null;
  const s = String(g);
  const m = s.match(/ProductVariant\/(\d+)/);
  return m ? m[1] : (s.match(/^\d+$/) ? s : null);
}

function parseRecipeFromValue(value, type) {
  // 1) Если это list.product_variant_reference → value как правило JSON или CSV GID’ов
  if (String(type || "").includes("product_variant_reference")) {
    try {
      const parsed = JSON.parse(value);
      const arr = Array.isArray(parsed) ? parsed : [parsed];
      const out = [];
      for (const it of arr) {
        const vid = parseGidVariantId(it?.id || it);
        if (vid) out.push({ variantId: vid, qty: 1 });
      }
      if (out.length) return out;
    } catch (_) {
      // попробуем CSV/строку
      const parts = String(value || "")
        .split(/[\s,;\|]+/).map(s => s.trim()).filter(Boolean);
      const out = [];
      for (const p of parts) {
        const vid = parseGidVariantId(p);
        if (vid) out.push({ variantId: vid, qty: 1 });
      }
      if (out.length) return out;
    }
  }

  // 2) Попытка как JSON со структурой объектов
  try {
    const parsed = JSON.parse(String(value));
    const arr = Array.isArray(parsed)
      ? parsed
      : (Array.isArray(parsed?.components) ? parsed.components : null);
    if (arr && arr.length) {
      const out = [];
      for (const it of arr) {
        const vid = parseGidVariantId(it?.variantId || it?.variant_id || it?.id);
        const qty = toNum(it?.qty || it?.quantity || it?.qty_each || 1) || 1;
        if (vid) out.push({ variantId: vid, qty });
      }
      if (out.length) return out;
    }
  } catch (_) {}

  // 3) Плоские пары "variantId:qty" в строке
  const flat = String(value || "");
  if (flat) {
    const out = [];
    for (const token of flat.split(/[,;\|]/)) {
      const m = String(token).trim()
        .match(/(?:ProductVariant\/)?(\d+)\s*:\s*(\d+)/i);
      if (m) out.push({ variantId: m[1], qty: toNum(m[2]) || 1 });
    }
    if (out.length) return out;
  }

  return null;
}

function pickRecipeFromMetafields(mfs) {
  if (!Array.isArray(mfs) || !mfs.length) return null;
  // Сначала пробуем приоритетные namespace/key
  for (const cand of SB_NS_CANDIDATES) {
    const mf = mfs.find(m => m.namespace === cand.ns && m.key === cand.key && m.value);
    if (mf) {
      const parsed = parseRecipeFromValue(mf.value, mf.type);
      if (parsed && parsed.length) return parsed;
    }
  }
  // Если нет — перебираем все метаполя в поиске похожих структур
  for (const mf of mfs) {
    if (!mf?.value) continue;
    const parsed = parseRecipeFromValue(mf.value, mf.type);
    if (parsed && parsed.length) return parsed;
  }
  return null;
}

// ---------- public API ----------
export async function dumpMeta(productId) {
  const product = await getProduct(productId);
  const pMfs = await listProductMetafields(productId);
  const variants = await listProductVariants(productId);

  const variantDump = {};
  for (const v of variants) {
    const mfs = await listVariantMetafields(v.id);
    variantDump[String(v.id)] = mfs;
  }

  return {
    ok: true,
    product_id: String(productId),
    product_recipe_guess: pickRecipeFromMetafields(pMfs) || [],
    product_metafields: pMfs,
    variants: variants.map(v => ({ id: v.id, sku: v.sku || null })),
    variant_metafields: variantDump
  };
}

export async function buildBundleMap({ onlyProductId = null, onlyVariantId = null } = {}) {
  const out = {}; // { "product:ID": [{variantId, qty}], "variant:ID": [...] }

  // Случай: только конкретный вариант — смотрим только его метаполя
  if (onlyVariantId) {
    const mfs = await listVariantMetafields(onlyVariantId);
    const recipe = pickRecipeFromMetafields(mfs);
    if (recipe && recipe.length) {
      out[`variant:${onlyVariantId}`] = recipe;
    }
    return out;
  }

  // Случай: конкретный продукт
  if (onlyProductId) {
    const pMfs = await listProductMetafields(onlyProductId);
    const pRecipe = pickRecipeFromMetafields(pMfs);
    if (pRecipe && pRecipe.length) {
      out[`product:${onlyProductId}`] = pRecipe;
    }

    const vars = await listProductVariants(onlyProductId);
    for (const v of vars) {
      const mfs = await listVariantMetafields(v.id);
      const r = pickRecipeFromMetafields(mfs);
      if (r && r.length) {
        out[`variant:${v.id}`] = r;
      }
    }
    return out;
  }

  // Если нужен полный скан — здесь можно пробежать все продукты магазина,
  // но это долго и требует пагинации. Оставим узкий путь через product_id/variant_id.
  return out;
}
