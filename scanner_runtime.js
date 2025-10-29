// scanner_runtime.js  (ESM)

// --- Настройки и известные метаполя Simple Bundles ---
const COMPONENT_FIELDS = [
  { ns: "simple_bundles_2_0", key: "components" },
  { ns: "simple_bundles",     key: "components" },
  { ns: "simplebundles",      key: "components" },
  { ns: "bundles",            key: "components" },
];

// Овверайды: какой продукт считать источником рецепта для другого
const OVERRIDES = {
  "product:10345847423286": "product:10353217306934",
};

// --- Вспомогательные ---
function toNum(x){ const n = Number(x); return Number.isFinite(n) ? n : 0; }
function shopHeaders(){
  const shop  = process.env.SHOPIFY_SHOP;
  const token = process.env.SHOPIFY_ADMIN_ACCESS_TOKEN;
  if (!shop || !token) throw new Error("SHOPIFY creds missing");
  return {
    shop,
    headers: { "X-Shopify-Access-Token": token, "Content-Type": "application/json" }
  };
}
async function fetchJson(path){
  const { shop, headers } = shopHeaders();
  const url = `https://${shop}${path}`;
  const r = await fetch(url, { headers });
  if (!r.ok) {
    const body = await r.text().catch(()=> "");
    throw new Error(`HTTP ${r.status} ${r.statusText} ${body}`);
  }
  return r.json();
}

function parseComponentsValue(val){
  if (!val) return [];
  const s = String(val).trim();
  const out = [];

  // JSON массив/объект
  if ((s.startsWith("[") && s.endsWith("]")) || (s.startsWith("{") && s.endsWith("}"))) {
    try {
      const parsed = JSON.parse(s);
      const arr = Array.isArray(parsed) ? parsed : (parsed.components || []);
      for (const it of arr) {
        const vid = String(it.variantId || it.variant_id || "").replace(/^gid:\/\/shopify\/ProductVariant\//, "");
        const qty = toNum(it.quantity ?? it.qty ?? it.qty_each ?? 0);
        if (vid && qty > 0) out.push({ variantId: vid, qty });
      }
    } catch(_) {}
  }

  // Плоский список "vid:qty" через запятые/точки с запятой/|
  const parts = s.split(/[,;|]/).map(x => x.trim()).filter(Boolean);
  for (const p of parts) {
    const m = p.match(/^(?:gid:\/\/shopify\/ProductVariant\/)?(\d+)\s*:\s*(\d+)$/i);
    if (m) out.push({ variantId: m[1], qty: toNum(m[2]) });
  }

  // дедупликация
  const map = new Map();
  for (const r of out) {
    if (!r.variantId || r.qty <= 0) continue;
    map.set(r.variantId, (map.get(r.variantId) || 0) + r.qty);
  }
  return [...map.entries()].map(([variantId, qty]) => ({ variantId, qty }));
}

// --- Загрузка метаполей ---
async function getProductMetafields(productId){
  const j = await fetchJson(`/admin/api/2025-01/products/${productId}/metafields.json`);
  return Array.isArray(j?.metafields) ? j.metafields : [];
}
async function getVariantMetafields(variantId){
  const j = await fetchJson(`/admin/api/2025-01/variants/${variantId}/metafields.json`);
  return Array.isArray(j?.metafields) ? j.metafields : [];
}
async function getProductVariants(productId){
  const j = await fetchJson(`/admin/api/2025-01/products/${productId}.json?fields=variants`);
  return Array.isArray(j?.product?.variants) ? j.product.variants : [];
}

// Парсим рецепт из набора метаполей
function extractRecipeFromMetafields(mfs){
  for (const f of COMPONENT_FIELDS) {
    const mf = mfs.find(m => m.namespace === f.ns && m.key === f.key && m.value);
    if (!mf) continue;
    const comps = parseComponentsValue(mf.value);
    if (comps.length) return comps;
  }
  return [];
}

// --- Кэш ---
const _bundleMapCache = new Map(); // key: productId string

function applyProductOverride(rawProductId){
  const k = `product:${String(rawProductId)}`;
  const mapped = OVERRIDES[k];
  if (!mapped) return { effectiveProductId: String(rawProductId), aliasFrom: null };
  const m = String(mapped);
  if (m.startsWith("product:")) return { effectiveProductId: m.slice("product:".length), aliasFrom: String(rawProductId) };
  return { effectiveProductId: m, aliasFrom: String(rawProductId) };
}

/**
 * Собирает карту рецептов:
 *  - ключи: "product:<id>" и "variant:<variant_id>"
 *  - значения: массив [{ variantId, qty }]
 * @param {{onlyProductId?: string|null}} opts
 * @returns {Promise<Record<string, Array<{variantId:string, qty:number}>>>}
 */
export async function buildBundleMap(opts = {}){
  const onlyProductId = opts.onlyProductId ? String(opts.onlyProductId) : null;

  // если просили конкретный продукт — применяем овверайд
  let scanProductId = null;
  let aliasFrom = null;
  if (onlyProductId) {
    const mapped = applyProductOverride(onlyProductId);
    scanProductId = mapped.effectiveProductId;
    aliasFrom = mapped.aliasFrom; // если отличается — ниже сделаем алиас
  }

  // кэш по конкретному продукту
  const cacheKey = scanProductId ? `pid:${scanProductId}` : "*ALL*";
  if (_bundleMapCache.has(cacheKey)) return _bundleMapCache.get(cacheKey);

  const out = {}; // { "product:ID": [...], "variant:VID":[...] }

  const productIds = [];
  if (scanProductId) {
    productIds.push(scanProductId);
  } else {
    // если нужен глобальный прогон — забираем продукты постранично (минимально)
    // тут можно расширить при необходимости
    // для текущей задачи достаточно работать по one-shot продукту
    // но оставим возможность расширения
  }

  // если нет конкретного продукта — выходим пустыми (чтобы не грузить всё)
  if (!productIds.length && !scanProductId) {
    _bundleMapCache.set(cacheKey, out);
    return out;
  }

  // сбор по каждому продукту
  for (const pid of productIds) {
    try {
      const mfs = await getProductMetafields(pid);
      const productRecipe = extractRecipeFromMetafields(mfs);

      // product-level рецепт (редко, но вдруг)
      if (productRecipe.length) {
        out[`product:${pid}`] = productRecipe;
      }

      // variant-level рецепты
      const variants = await getProductVariants(pid);
      for (const v of variants) {
        try {
          const vId = String(v.id);
          const vmf = await getVariantMetafields(vId);
          const vRecipe = extractRecipeFromMetafields(vmf);
          if (vRecipe.length) {
            out[`variant:${vId}`] = vRecipe;
          }
        } catch (e) {
          // пропускаем конкретный вариант
        }
      }

      // если запрошен алиас (овверайд) — продублируем ключ product:<aliasFrom> к product:<pid>
      if (aliasFrom) {
        const srcKey = `product:${pid}`;
        const dstKey = `product:${aliasFrom}`;
        if (out[srcKey] && !out[dstKey]) {
          out[dstKey] = out[srcKey];
        }
      }

    } catch (e) {
      // пропускаем продукт
    }
  }

  _bundleMapCache.set(cacheKey, out);
  return out;
}

/**
 * Диагностический дамп метаполей по продукту + вариантам
 * @param {string} productId
 * @returns {Promise<object>}
 */
export async function dumpMeta(productId){
  const pid = String(productId);
  try {
    const mfs = await getProductMetafields(pid);
    const product_recipe = extractRecipeFromMetafields(mfs);

    const variants = await getProductVariants(pid);
    const variant_recipes = {};
    for (const v of variants) {
      try {
        const vmf = await getVariantMetafields(String(v.id));
        variant_recipes[String(v.id)] = extractRecipeFromMetafields(vmf);
      } catch {
        variant_recipes[String(v.id)] = [];
      }
    }

    return {
      ok: true,
      product_id: pid,
      product_recipe,
      variants: variants.map(v => ({ id: v.id })),
      variant_recipes
    };
  } catch (e) {
    return { ok: false, error: String(e.message || e), product_id: pid };
  }
}
