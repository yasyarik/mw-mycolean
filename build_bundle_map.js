// build_bundle_map.js
// Собирает карту рецептов бандлов из метаполей продукта/варианта и сохраняет в bundle-map.json

const SHOP = process.env.SHOPIFY_SHOP;
const TOKEN = process.env.SHOPIFY_ADMIN_ACCESS_TOKEN;

if (!SHOP || !TOKEN) {
  console.error("ERROR: set SHOPIFY_SHOP and SHOPIFY_ADMIN_ACCESS_TOKEN env vars.");
  process.exit(1);
}

const RECIPE_NS_KEYS = [
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
      try { val = JSON.parse(s); } catch { /* ignore */ }
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
  const res = await fetch(`https://${SHOP}/admin/api/2025-01/graphql.json`, {
    method: "POST",
    headers: {
      "X-Shopify-Access-Token": TOKEN,
      "Content-Type": "application/json"
    },
    body
  });
  if (!res.ok) {
    const t = await res.text().catch(()=>"");
    throw new Error(`GraphQL HTTP ${res.status} ${res.statusText} ${t}`);
  }
  const j = await res.json();
  if (j.errors) {
    throw new Error(`GraphQL errors: ${JSON.stringify(j.errors).slice(0,500)}`);
  }
  return j;
}

// Можно ограничить сбор по конкретному productId для быстрого теста:
// запусти так: PRODUCT_ID=10345847423286 node build_bundle_map.js
const ONLY_PRODUCT_ID = process.env.PRODUCT_ID ? String(process.env.PRODUCT_ID) : null;

const META_IDENTIFIERS = RECIPE_NS_KEYS.map(k => ({ namespace: k.ns, key: k.key }));

const QUERY = `
query BundleScan($first:Int!, $after:String, $ids:[HasMetafieldsIdentifier!]!, $productId:ID) {
  products(first:$first, after:$after, ${ONLY_PRODUCT_ID ? "query:\"id:"+ONLY_PRODUCT_ID+"\"" : ""}) {
    edges {
      cursor
      node {
        id
        title
        metafields(identifiers:$ids){ namespace key type value }
        variants(first: 100) {
          edges {
            node {
              id
              sku
              metafields(identifiers:$ids){ namespace key type value }
            }
          }
        }
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}
`;

async function fetchAllProducts() {
  const all = [];
  let after = null;
  let page = 0;
  do {
    const body = gqlBody(QUERY, { first: 50, after, ids: META_IDENTIFIERS });
    const data = await gqlFetch(body);
    const edges = data?.data?.products?.edges || [];
    const pageInfo = data?.data?.products?.pageInfo || {};
    for (const e of edges) all.push(e.node);
    after = pageInfo?.hasNextPage ? pageInfo.endCursor : null;
    page++;
    console.log(`Fetched page ${page}, items: ${edges.length}, hasNext: ${!!after}`);
  } while (after);
  return all;
}

function collectRecipes(product) {
  const pid = product.id.replace(/^gid:\/\/shopify\/Product\//, "");
  const map = [];

  // product-level
  const pmf = Array.isArray(product.metafields) ? product.metafields : [];
  for (const mf of pmf) {
    const arr = parseRecipeArray(mf?.value);
    if (arr.length) {
      map.push({ key: `product:${pid}`, from: `${mf.namespace}/${mf.key}`, components: arr });
      break; // берём первый валидный
    }
  }

  // variant-level
  const vEdges = product?.variants?.edges || [];
  for (const ve of vEdges) {
    const v = ve.node;
    const vid = v.id.replace(/^gid:\/\/shopify\/ProductVariant\//, "");
    const vmf = Array.isArray(v.metafields) ? v.metafields : [];
    for (const mf of vmf) {
      const arr = parseRecipeArray(mf?.value);
      if (arr.length) {
        map.push({ key: `variant:${vid}`, from: `${mf.namespace}/${mf.key}`, components: arr });
        break;
      }
    }
  }

  return map;
}

async function main() {
  console.time("bundle-scan");
  try {
    const products = await fetchAllProducts();
    console.log(`Total products fetched: ${products.length}`);

    const bundleMap = {};
    let hits = 0;

    for (const p of products) {
      const recs = collectRecipes(p);
      if (!recs.length) continue;
      for (const r of recs) {
        bundleMap[r.key] = r.components;
        hits++;
        console.log(`[FOUND] ${r.key} via ${r.from} -> ${r.components.length} items`);
      }
    }

    const fs = await import('fs');
    fs.writeFileSync('bundle-map.json', JSON.stringify(bundleMap, null, 2));
    const keys = Object.keys(bundleMap);
    console.log(`\nSaved bundle-map.json with ${keys.length} keys (recipes found: ${hits}).`);
    if (keys.length) {
      console.log(`Example key: ${keys[0]} ->`, bundleMap[keys[0]]);
    } else {
      console.log("No recipes found. Check namespaces/keys or app writes.");
    }
  } catch (e) {
    console.error("SCAN ERROR:", e.message);
    process.exit(1);
  } finally {
    console.timeEnd("bundle-scan");
  }
}

main();

