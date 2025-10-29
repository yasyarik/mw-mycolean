// scanner_runtime.js
export const RECIPE_NS_KEYS = [
  { ns: "simple_bundles_2_0", key: "components" },
  { ns: "simple_bundles",     key: "components" },
  { ns: "simplebundles",      key: "components" },
  { ns: "bundles",            key: "components" },
  { ns: "sb",                 key: "components" },
  { ns: "simple_bundles_2_0", key: "bundle_components" },
  { ns: "simple_bundles",     key: "bundle_components" },
  { ns: "bundles",            key: "bundle_components" },
  { ns: "simple_bundles_2_0", key: "components_json" },
  { ns: "simple_bundles",     key: "components_json" },
  { ns: "bundles",            key: "components_json" }
];

function parseRecipeArray(raw) {
  if (!raw) return [];
  let val = raw;
  if (typeof val === "string") {
    const s = val.trim();
    if ((s.startsWith("[") && s.endsWith("]")) || (s.startsWith("{") && s.endsWith("}"))) {
      try { val = JSON.parse(s); } catch {}
    }
  }
  const list = Array.isArray(val) ? val : (val && Array.isArray(val.components) ? val.components : []);
  const out = [];
  for (const it of list) {
    if (!it) continue;
    const vid = String(it.variantId || it.variant_id || "").replace(/^gid:\/\/shopify\/ProductVariant\//, "");
    const qty = Number(it.quantity || it.qty || it.qty_each || it.count || 0);
    if (vid && qty > 0) out.push({ variantId: vid, qty });
  }
  return out;
}

function gqlBody(query, variables) {
  return JSON.stringify({ query, variables });
}

async function gqlFetch(body) {
  const shop = process.env.SHOPIFY_SHOP;
  const token = process.env.SHOPIFY_ADMIN_ACCESS_TOKEN;
  const r = await fetch(`https://${shop}/admin/api/2025-01/graphql.json`, {
    method: "POST",
    headers: { "X-Shopify-Access-Token": token, "Content-Type": "application/json" },
    body
  });
  if (!r.ok) {
    const t = await r.text().catch(()=> "");
    throw new Error(`GraphQL HTTP ${r.status} ${r.statusText} ${t}`);
  }
  const j = await r.json();
  if (j.errors) throw new Error(`GraphQL errors: ${JSON.stringify(j.errors).slice(0,500)}`);
  return j;
}

export async function buildBundleMap({ onlyProductId = null } = {}) {
  const ids = RECIPE_NS_KEYS.map(k => ({ namespace: k.ns, key: k.key }));
  const query = `
  query BundleScan($first:Int!, $after:String, $ids:[HasMetafieldsIdentifier!]!) {
    products(first:$first, after:$after${onlyProductId ? `, query:"id:${onlyProductId}"` : ""}) {
      edges {
        cursor
        node {
          id
          metafields(identifiers:$ids){ namespace key value type }
          variants(first: 100) {
            edges {
              node {
                id
                sku
                metafields(identifiers:$ids){ namespace key value type }
              }
            }
          }
        }
      }
      pageInfo { hasNextPage endCursor }
    }
  }`;

  const all = [];
  let after = null;
  do {
    const body = gqlBody(query, { first: 50, after, ids });
    const data = await gqlFetch(body);
    const edges = data?.data?.products?.edges || [];
    for (const e of edges) all.push(e.node);
    const pageInfo = data?.data?.products?.pageInfo || {};
    after = pageInfo?.hasNextPage ? pageInfo.endCursor : null;
  } while (after);

  const map = {};
  for (const p of all) {
    const pid = String(p.id).replace(/^gid:\/\/shopify\/Product\//, "");
    const pmf = Array.isArray(p.metafields) ? p.metafields : [];
    for (const mf of pmf) {
      const arr = parseRecipeArray(mf?.value);
      if (arr.length) { map[`product:${pid}`] = arr; break; }
    }
    const vEdges = p?.variants?.edges || [];
    for (const ve of vEdges) {
      const v = ve.node;
      const vid = String(v.id).replace(/^gid:\/\/shopify\/ProductVariant\//, "");
      const vmf = Array.isArray(v.metafields) ? v.metafields : [];
      for (const mf of vmf) {
        const arr = parseRecipeArray(mf?.value);
        if (arr.length) { map[`variant:${vid}`] = arr; break; }
      }
    }
  }
  return map;
}
