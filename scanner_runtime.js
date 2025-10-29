const NS_KEYS = [
  { ns: "simple_bundles_2_0", key: "components" },
  { ns: "simple_bundles",     key: "components" },
  { ns: "simplebundles",      key: "components" },
  { ns: "bundles",            key: "components" }
];

function toStr(x){ return x==null ? "" : String(x); }
function asArr(x){ return Array.isArray(x) ? x : []; }

function parseRecipeFromJSON(val){
  try{
    const parsed = typeof val === "string" ? JSON.parse(val) : val;
    const list = Array.isArray(parsed) ? parsed : Array.isArray(parsed?.components) ? parsed.components : [];
    const out=[];
    for(const it of list){
      const vid = toStr(it.variantId || it.variant_id || "").replace(/^gid:\/\/shopify\/ProductVariant\//,"");
      const qty = Number(it.quantity ?? it.qty ?? it.qty_each ?? 0);
      if(vid && qty>0) out.push({ variantId: vid, qty });
    }
    return out;
  }catch{ return []; }
}

function pickComponentsFromMetafields(metafields){
  for(const mf of asArr(metafields)){
    const ns = mf.namespace, key = mf.key;
    if(NS_KEYS.some(k => k.ns===ns && k.key===key)){
      const out = parseRecipeFromJSON(mf.value);
      if(out.length) return out;
    }
  }
  for(const mf of asArr(metafields)){
    const out = parseRecipeFromJSON(mf.value);
    if(out.length) return out;
  }
  return [];
}

async function apiJSON(url, token){
  const r = await fetch(url,{ headers:{ "X-Shopify-Access-Token": token }});
  if(!r.ok) return null;
  return await r.json();
}

export async function readProductComponents(shop, token, productId){
  const j = await apiJSON(`https://${shop}/admin/api/2025-01/products/${productId}/metafields.json`, token);
  if(!j) return [];
  return pickComponentsFromMetafields(j.metafields);
}

export async function readVariantComponents(shop, token, variantId){
  const j = await apiJSON(`https://${shop}/admin/api/2025-01/variants/${variantId}/metafields.json`, token);
  if(!j) return [];
  return pickComponentsFromMetafields(j.metafields);
}

async function listProductWithVariants(shop, token, productId){
  const j = await apiJSON(`https://${shop}/admin/api/2025-01/products/${productId}.json?fields=id,variants`, token);
  return j?.product ? j.product : { id: productId, variants: [] };
}

async function listProductsChunk(shop, token, sinceId){
  const qs = new URLSearchParams({ limit:"250", fields:"id,variants" });
  if(sinceId) qs.set("since_id", String(sinceId));
  const j = await apiJSON(`https://${shop}/admin/api/2025-01/products.json?${qs}`, token);
  return { products: asArr(j?.products) };
}

export async function buildBundleMap({ onlyProductId=null, onlyVariantId=null } = {}){
  const shop = process.env.SHOPIFY_SHOP;
  const token = process.env.SHOPIFY_ADMIN_ACCESS_TOKEN;
  const out={};
  if(!shop || !token) return out;

  async function addVariant(vid){
    const comps = await readVariantComponents(shop, token, vid);
    if(comps.length) out[`variant:${vid}`]=comps;
  }

  if(onlyVariantId){
    await addVariant(String(onlyVariantId));
    return out;
  }

  if(onlyProductId){
    const pid = String(onlyProductId);
    const prodComps = await readProductComponents(shop, token, pid);
    if(prodComps.length) out[`product:${pid}`]=prodComps;

    const p = await listProductWithVariants(shop, token, pid);
    for(const v of asArr(p.variants)){
      if(v?.id) await addVariant(String(v.id));
    }
    return out;
  }

  let since=null;
  while(true){
    const { products } = await listProductsChunk(shop, token, since);
    if(!products.length) break;
    for(const p of products){
      const pid = String(p.id);
      const prodComps = await readProductComponents(shop, token, pid);
      if(prodComps.length) out[`product:${pid}`]=prodComps;
      for(const v of asArr(p.variants)){
        if(v?.id) await addVariant(String(v.id));
      }
      since = p.id;
    }
    if(products.length<250) break;
  }
  return out;
}

export async function dumpMeta(productId){
  const shop = process.env.SHOPIFY_SHOP;
  const token = process.env.SHOPIFY_ADMIN_ACCESS_TOKEN;
  if(!shop || !token) return { ok:false, error:"missing_creds" };
  const pid = String(productId||"");
  if(!pid) return { ok:false, error:"missing_product_id" };

  const prod = await apiJSON(`https://${shop}/admin/api/2025-01/products/${pid}.json?fields=id,variants`, token);
  const product = prod?.product || { id: pid, variants: [] };

  const prodMf = await apiJSON(`https://${shop}/admin/api/2025-01/products/${pid}/metafields.json`, token);
  const prodRecipe = pickComponentsFromMetafields(prodMf?.metafields||[]);

  const variantRecipes = {};
  for(const v of asArr(product.variants)){
    const vmf = await apiJSON(`https://${shop}/admin/api/2025-01/variants/${v.id}/metafields.json`, token);
    variantRecipes[String(v.id)] = pickComponentsFromMetafields(vmf?.metafields||[]);
  }

  return {
    ok:true,
    product_id: pid,
    product_recipe: prodRecipe,
    variants: asArr(product.variants).map(v=>({ id:v.id })),
    variant_recipes: variantRecipes
  };
}
