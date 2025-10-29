// scanner_runtime.js  (ESM)
const SHOP = process.env.SHOPIFY_SHOP;
const ADMIN = process.env.SHOPIFY_ADMIN_ACCESS_TOKEN;

if (!globalThis.fetch) {
  // Node 18+ есть fetch; на всякий случай
  globalThis.fetch = (await import('node-fetch')).default;
}

// Оверрайды связок (ручные маппинги)
const ID_ALIAS = {
  "product:10345847423286": "product:10353217306934",
};

function keyFor(type, id) {
  return `${type}:${String(id)}`;
}

function normJSON(v) {
  if (v == null) return null;
  if (typeof v === "object") return v;
  const s = String(v).trim();
  if (!s) return null;
  try { return JSON.parse(s); } catch (_) { return s; }
}

function tryParsePairs(str) {
  if (typeof str !== "string") return [];
  // Поддержка форматов:
  // 1) JSON-массив объектов: [{variantId:"...", qty:1}, ...]
  // 2) CSV по строкам: variantId,qty
  // 3) Список "variantId:qty" через запятую
  const out = [];

  // JSON-массив
  try {
    const j = JSON.parse(str);
    if (Array.isArray(j)) {
      j.forEach(x => {
        const vid = String(x.variantId || x.variant_id || "").trim();
        const qty = Number(x.qty || x.quantity || 1) || 1;
        if (vid) out.push({ variantId: vid, qty });
      });
      if (out.length) return out;
    }
  } catch (_) {}

  // CSV: lines of "vid,qty"
  const lines = str.split(/\r?\n/).map(s => s.trim()).filter(Boolean);
  if (lines.length > 1 && lines.every(l => /,/.test(l))) {
    lines.forEach(l => {
      const [vid, q] = l.split(",").map(s => s.trim());
      if (vid) out.push({ variantId: vid, qty: Number(q) || 1 });
    });
    if (out.length) return out;
  }

  // list "vid:qty,vid:qty"
  if (str.includes(":")) {
    str.split(",").map(s => s.trim()).forEach(tok => {
      const [vid, q] = tok.split(":").map(s => s.trim());
      if (vid) out.push({ variantId: vid, qty: Number(q) || 1 });
    });
    if (out.length) return out;
  }

  // Если просто одно значение — трактуем как "variantId"
  if (str && !out.length) out.push({ variantId: str, qty: 1 });
  return out;
}

async function shopGetJson(path) {
  if (!SHOP || !ADMIN) throw new Error("Missing SHOPIFY_SHOP or SHOPIFY_ADMIN_ACCESS_TOKEN");
  const url = `https://${SHOP}/admin/api/2025-01${path}`;
  const r = await fetch(url, { headers: { "X-Shopify-Access-Token": ADMIN } });
  if (!r.ok) {
    const txt = await r.text().catch(()=> "");
    throw new Error(`HTTP ${r.status} ${r.statusText} ${txt}`);
  }
  return r.json();
}

// читаем метаполя продукта
async function fetchProductMetafields(productId) {
  const j = await shopGetJson(`/products/${productId}/metafields.json`);
  return Array.isArray(j?.metafields) ? j.metafields : [];
}

// читаем метаполя варианта
async function fetchVariantMetafields(variantId) {
  const j = await shopGetJson(`/variants/${variantId}/metafields.json`);
  return Array.isArray(j?.metafields) ? j.metafields : [];
}

function extractChildrenFromMetafields(mfs) {
  const pairs = [];

  for (const mf of mfs) {
    const key = `${mf.namespace}.${mf.key}`.toLowerCase();
    const val = normJSON(mf.value);

    // Наиболее типовые ключи, где приложения хранят рецепт
    const likely =
      key.includes("bundle") ||
      key.includes("children") ||
      key.includes("components") ||
      key.includes("skio") ||
      key.includes("simple") ||
      key.includes("recipe");

    if (!likely) continue;

    if (Array.isArray(val)) {
      val.forEach(x => {
        const vid = String(x?.variantId || x?.variant_id || "").trim();
        const qty = Number(x?.qty || x?.quantity || 1) || 1;
        if (vid) pairs.push({ variantId: vid, qty });
      });
    } else if (typeof val === "string") {
      pairs.push(...tryParsePairs(val));
    } else if (val && typeof val === "object") {
      const maybeArr = val.variants || val.children || val.items || val.components;
      if (Array.isArray(maybeArr)) {
        maybeArr.forEach(x => {
          const vid = String(x?.variantId || x?.variant_id || "").trim();
          const qty = Number(x?.qty || x?.quantity || 1) || 1;
          if (vid) pairs.push({ variantId: vid, qty });
        });
      }
    }
  }

  // Убираем мусор вида variantId:"09"
  return pairs.filter(p => /^\d+$/.test(String(p.variantId)));
}

// Публичные функции

export async function dumpMeta(productId) {
  const pid = String(productId);
  const meta = await fetchProductMetafields(pid);
  const variantsResp = await shopGetJson(`/products/${pid}.json?fields=variants`);
  const variants = variantsResp?.product?.variants || [];
  const vMeta = {};
  for (const v of variants) {
    const arr = await fetchVariantMetafields(v.id);
    vMeta[String(v.id)] = arr;
  }
  return { ok: true, product_id: pid, product_metafields: meta, variant_metafields: vMeta };
}

export async function buildBundleMap(opts = {}) {
  const { onlyProductId = null } = opts; // ← фикс undefined
  const map = {};

  // Один продукт — быстрый путь
  if (onlyProductId) {
    const aliasKey = ID_ALIAS[keyFor("product", onlyProductId)] || keyFor("product", onlyProductId);
    const prodId = aliasKey.startsWith("product:") ? aliasKey.split(":")[1] : String(onlyProductId);

    const pmf = await fetchProductMetafields(prodId);
    const pChildren = extractChildrenFromMetafields(pmf);
    if (pChildren.length) {
      map[keyFor("product", prodId)] = pChildren;
    }

    const variantsResp = await shopGetJson(`/products/${prodId}.json?fields=variants`);
    const variants = variantsResp?.product?.variants || [];
    for (const v of variants) {
      const vmf = await fetchVariantMetafields(v.id);
      const vChildren = extractChildrenFromMetafields(vmf);
      if (vChildren.length) {
        map[keyFor("variant", v.id)] = vChildren;
      }
    }
    return map;
  }

  // Полный обход каталога (постранично)
  let pageInfo = null;
  for (let page = 0; page < 50; page++) {
    const qp = pageInfo ? `&page_info=${encodeURIComponent(pageInfo)}` : "";
    const resp = await shopGetJson(`/products.json?limit=250${qp}&fields=id,variants`);
    const products = resp?.products || [];
    for (const p of products) {
      const aliasKey = ID_ALIAS[keyFor("product", p.id)] || keyFor("product", p.id);
      const pid = aliasKey.startsWith("product:") ? aliasKey.split(":")[1] : String(p.id);

      const pmf = await fetchProductMetafields(pid);
      const pChildren = extractChildrenFromMetafields(pmf);
      if (pChildren.length) {
        map[keyFor("product", pid)] = pChildren;
      }

      for (const v of (p.variants || [])) {
        const vmf = await fetchVariantMetafields(v.id);
        const vChildren = extractChildrenFromMetafields(vmf);
        if (vChildren.length) {
          map[keyFor("variant", v.id)] = vChildren;
        }
      }
    }

    // Shipify REST пагинация: Link header — в проде можно дочинить при необходимости.
    // Здесь — выходим после одной страницы, чтобы не долбить API.
    break;
  }

  return map;
}
