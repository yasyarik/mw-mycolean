const NS_KEYS = [
  { ns: "simple_bundles_2_0", key: "components" },
  { ns: "simple_bundles", key: "components" },
  { ns: "simplebundles", key: "components" },
  { ns: "bundles", key: "components" }
];

function toStr(x) {
  return typeof x === "string" ? x : x == null ? "" : String(x);
}

function parseComponentsValue(val) {
  if (!val) return [];
  try {
    const parsed = JSON.parse(val);
    const list = Array.isArray(parsed) ? parsed : Array.isArray(parsed.components) ? parsed.components : [];
    const out = [];
    for (const it of list) {
      const vid = toStr(it.variantId || it.variant_id || "").replace(/^gid:\/\/shopify\/ProductVariant\//, "");
      const qty = Number(it.quantity || it.qty || it.qty_each || 0);
      if (vid && qty > 0) out.push({ variantId: vid, qty });
    }
    return out;
  } catch {
    return [];
  }
}

async function readVariantComponents(shop, token, variantId) {
  const url = `https://${shop}/admin/api/2025-01/variants/${variantId}/metafields.json`;
  const r = await fetch(url, { headers: { "X-Shopify-Access-Token": token } });
  if (!r.ok) return [];
  const j = await r.json();
  const mfs = Array.isArray(j.metafields) ? j.metafields : [];
  for (const mf of mfs) {
    const ns = mf.namespace;
    const key = mf.key;
    if (NS_KEYS.some(k => k.ns === ns && k.key === key)) {
      const comps = parseComponentsValue(mf.value);
      if (comps.length) return comps;
    }
  }
  return [];
}

async function readProductComponents(shop, token, productId) {
  const url = `https://${shop}/admin/api/2025-01/products/${productId}/metafields.json`;
  const r = await fetch(url, { headers: { "X-Shopify-Access-Token": token } });
  if (!r.ok) return [];
  const j = await r.json();
  const mfs = Array.isArray(j.metafields) ? j.metafields : [];
  for (const mf of mfs) {
    const ns = mf.namespace;
    const key = mf.key;
    if (NS_KEYS.some(k => k.ns === ns && k.key === key)) {
      const comps = parseComponentsValue(mf.value);
      if (comps.length) return comps;
    }
  }
  return [];
}

async function listProductsChunk(shop, token, sinceId) {
  const qs = new URLSearchParams();
  qs.set("limit", "250");
  qs.set("fields", "id,variants");
  if (sinceId) qs.set("since_id", String(sinceId));
  const url = `https://${shop}/admin/api/2025-01/products.json?${qs.toString()}`;
  const r = await fetch(url, { headers: { "X-Shopify-Access-Token": token } });
  if (!r.ok) return { products: [] };
  const j = await r.json();
  return { products: Array.isArray(j.products) ? j.products : [] };
}

export async function buildBundleMap({ onlyProductId = null } = {}) {
  const shop = process.env.SHOPIFY_SHOP;
  const token = process.env.SHOPIFY_ADMIN_ACCESS_TOKEN;
  const out = {};
  if (!shop || !token) return out;

  async function addVariantRecipe(vid) {
    const comps = await readVariantComponents(shop, token, vid);
    if (comps.length) out[`variant:${vid}`] = comps;
  }

  if (onlyProductId) {
    const pid = String(onlyProductId);
    const prodComps = await readProductComponents(shop, token, pid);
    if (prodComps.length) out[`product:${pid}`] = prodComps;

    const r = await fetch(`https://${shop}/admin/api/2025-01/products/${pid}.json?fields=id,variants`, {
      headers: { "X-Shopify-Access-Token": token }
    });
    if (r.ok) {
      const j = await r.json();
      const vars = j?.product?.variants || [];
      for (const v of vars) {
        if (v?.id) await addVariantRecipe(String(v.id));
      }
    }
    return out;
  }

  let since = null;
  while (true) {
    const { products } = await listProductsChunk(shop, token, since);
    if (!products.length) break;
    for (const p of products) {
      const pid = String(p.id);
      const prodComps = await readProductComponents(shop, token, pid);
      if (prodComps.length) out[`product:${pid}`] = prodComps;
      const vars = Array.isArray(p.variants) ? p.variants : [];
      for (const v of vars) {
        if (v?.id) await addVariantRecipe(String(v.id));
      }
      since = p.id;
    }
    if (products.length < 250) break;
  }

  return out;
}

export async function dumpMeta(productId) {
  const shop = process.env.SHOPIFY_SHOP;
  const token = process.env.SHOPIFY_ADMIN_ACCESS_TOKEN;
  const pid = String(productId || "");
  if (!shop || !token || !pid) return { ok: false, error: "missing" };

  const prodMfs = await readProductComponents(shop, token, pid);
  let variants = [];
  let variantMfs = {};
  const r = await fetch(`https://${shop}/admin/api/2025-01/products/${pid}.json?fields=id,variants`, {
    headers: { "X-Shopify-Access-Token": token }
  });
  if (r.ok) {
    const j = await r.json();
    variants = j?.product?.variants || [];
    for (const v of variants) {
      if (!v?.id) continue;
      const vid = String(v.id);
      const comps = await readVariantComponents(shop, token, vid);
      variantMfs[vid] = comps;
    }
  }

  return {
    ok: true,
    product_id: pid,
    product_components: prodMfs,
    variants: variants.map(v => ({ id: v.id })),
    variant_components: variantMfs
  };
}
