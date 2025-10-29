const SHOP = process.env.SHOPIFY_SHOP;
const TOKEN = process.env.SHOPIFY_ADMIN_ACCESS_TOKEN;

const SB_NS_CANDIDATES = [
  { ns: "simple_bundles_2_0", key: "components" },
  { ns: "simple_bundles",     key: "components" },
  { ns: "simplebundles",      key: "components" },
  { ns: "bundles",            key: "components" }
];
// Product-level overrides: read recipe from another product, but store under the original
const PRODUCT_RECIPE_OVERRIDES = {
  "10345847423286": "10353217306934"
};


function toNum(x){ const n=Number(x); return Number.isFinite(n)?n:0; }

async function adminFetch(path){
  const url = `https://${SHOP}/admin/api/2025-01${path}`;
  const r = await fetch(url,{ headers:{ "X-Shopify-Access-Token":TOKEN, "Content-Type":"application/json" }});
  if(!r.ok){
    const t = await r.text().catch(()=> "");
    throw new Error(`HTTP ${r.status} ${r.statusText} ${t}`);
  }
  return r;
}

async function listAllProducts(){
  const out=[]; let since=0;
  for(;;){
    const r = await adminFetch(`/products.json?fields=id&limit=250${since?`&since_id=${since}`:""}`);
    const j = await r.json();
    const arr = Array.isArray(j.products)?j.products:[];
    if(!arr.length) break;
    for(const p of arr) out.push({id:p.id});
    since = arr[arr.length-1].id;
    if(arr.length<250) break;
  }
  return out;
}

async function listProductVariants(productId){
  const r = await adminFetch(`/products/${productId}.json?fields=variants`);
  const j = await r.json();
  const v = Array.isArray(j.product?.variants)?j.product.variants:[];
  return v.map(x=>({id:x.id}));
}

async function listProductMetafields(productId){
  let out=[]; let page=null;
  for(;;){
    const r = await adminFetch(`/products/${productId}/metafields.json?limit=250${page?`&page_info=${encodeURIComponent(page)}`:""}`);
    const j = await r.json();
    const arr = Array.isArray(j.metafields)?j.metafields:[];
    out = out.concat(arr.map(m=>({namespace:m.namespace,key:m.key,value:m.value,type:m.type})));
    const link=r.headers.get("link")||"";
    const m=link.match(/<[^>]*page_info=([^&>]+)[^>]*>; rel="next"/i);
    if(m){ page=m[1]; continue; }
    break;
  }
  return out;
}

async function listVariantMetafields(variantId){
  let out=[]; let page=null;
  for(;;){
    const r = await adminFetch(`/variants/${variantId}/metafields.json?limit=250${page?`&page_info=${encodeURIComponent(page)}`:""}`);
    const j = await r.json();
    const arr = Array.isArray(j.metafields)?j.metafields:[];
    out = out.concat(arr.map(m=>({namespace:m.namespace,key:m.key,value:m.value,type:m.type})));
    const link=r.headers.get("link")||"";
    const m=link.match(/<[^>]*page_info=([^&>]+)[^>]*>; rel="next"/i);
    if(m){ page=m[1]; continue; }
    break;
  }
  return out;
}

/* === Ослабленная валидация: принимаем любое большое число или gid === */
function extractVariantId(raw){
  if (!raw) return null;
  const s = String(raw).trim();
  const m1 = s.match(/ProductVariant\/(\d+)/i);
  if (m1) return m1[1];
  const m2 = s.match(/^(\d{6,})$/); // 6+ цифр
  return m2 ? m2[1] : null;
}

/* Без knownVariantIds: дети могут быть из других продуктов */
function normalizeRecipe(list){
  const out=[];
  for(const it of list||[]){
    const vid = extractVariantId(it.variantId ?? it.variant_id ?? it.id);
    const qty = Math.max(1, Number(it.qty ?? it.quantity ?? it.qty_each ?? 1));
    if(vid) out.push({ variantId: vid, qty });
  }
  return out;
}

function parseRecipeFromValue(value, type){
  // product_variant_reference / list.product_reference.* — достаём gid/числа
  if (String(type||"").includes("product_variant_reference")) {
    try{
      const parsed = JSON.parse(String(value));
      const arr = Array.isArray(parsed)?parsed:[parsed];
      const tmp=[];
      for(const it of arr){
        const vid = extractVariantId(it?.id ?? it);
        if(vid) tmp.push({ variantId: vid, qty: 1 });
      }
      return normalizeRecipe(tmp);
    }catch(_){
      const tmp=[];
      for(const token of String(value||"").split(/[\s,;|]+/)){
        const vid = extractVariantId(token);
        if(vid) tmp.push({ variantId: vid, qty: 1 });
      }
      return normalizeRecipe(tmp);
    }
  }

  // JSON массив/объект с components
  try{
    const parsed = JSON.parse(String(value));
    const arr = Array.isArray(parsed) ? parsed
              : (Array.isArray(parsed?.components)? parsed.components : null);
    if(arr && arr.length){
      const tmp=[];
      for(const it of arr){
        const vid = extractVariantId(it?.variantId ?? it?.variant_id ?? it?.id);
        const qty = Math.max(1, Number(it?.qty ?? it?.quantity ?? it?.qty_each ?? 1));
        if(vid) tmp.push({ variantId: vid, qty });
      }
      return normalizeRecipe(tmp);
    }
  }catch(_){}

  // Плоская строка вида "12345:2,67890:1" или с gid
  const flat = String(value||"");
  if(flat){
    const tmp=[];
    for(const token of flat.split(/[,;|]/)){
      const t = token.trim();
      const m = t.match(/(?:ProductVariant\/)?(\d{6,})\s*:\s*(\d+)/i);
      if(m) tmp.push({ variantId: m[1], qty: Number(m[2])||1 });
      else{
        const vid = extractVariantId(t);
        if(vid) tmp.push({ variantId: vid, qty: 1 });
      }
    }
    return normalizeRecipe(tmp);
  }
  return [];
}

function pickRecipeFromMetafields(mfs){
  if(!Array.isArray(mfs) || !mfs.length) return [];
  for(const cand of SB_NS_CANDIDATES){
    const mf = mfs.find(m=>m.namespace===cand.ns && m.key===cand.key && m.value);
    if(mf){
      const r = parseRecipeFromValue(mf.value, mf.type);
      if(r.length) return r;
    }
  }
  for(const mf of mfs){
    if(!mf?.value) continue;
    const r = parseRecipeFromValue(mf.value, mf.type);
    if(r.length) return r;
  }
  return [];
}

/* ===== Публичные функции ===== */

export async function dumpMeta(productId){
  const variants = await listProductVariants(productId);
  const product_metafields = await listProductMetafields(productId);
  const product_recipe = pickRecipeFromMetafields(product_metafields);

  const variant_recipes = {};
  for(const v of variants){
    const m = await listVariantMetafields(v.id);
    variant_recipes[v.id] = pickRecipeFromMetafields(m);
  }

  return {
    ok:true,
    product_id:String(productId),
    product_recipe,
    variants: variants.map(v=>({id:v.id})),
    variant_recipes,
    product_metafields
  };
}

export async function buildBundleMap({ onlyProductId=null, onlyVariantId=null } = {}){
  const out={};

  if(onlyVariantId){
    const r = await adminFetch(`/variants/${onlyVariantId}.json?fields=product_id`);
    const j = await r.json();
    if(!j?.variant?.product_id) return out;
    const mfs = await listVariantMetafields(onlyVariantId);
    const rec = pickRecipeFromMetafields(mfs);
    if(rec.length) out[`variant:${onlyVariantId}`]=rec;
    return out;
  }

if(onlyProductId){
  const scanProductId = PRODUCT_RECIPE_OVERRIDES[String(onlyProductId)] || onlyProductId;

  // читаем МФ у "донорского" продукта (scanProductId), но сохраняем под ключом оригинала
  const pMfs = await listProductMetafields(scanProductId);
  const pRec = pickRecipeFromMetafields(pMfs);
  if(pRec.length) out[`product:${onlyProductId}`]=pRec;

  // варианты читаем у того же "донорского" продукта, но храним как варианты оригинала не нужно.
  // Для совместимости — просто добавим варианты донора как есть:
  const vars = await listProductVariants(scanProductId);
  for(const v of vars){
    const mfs = await listVariantMetafields(v.id);
    const r = pickRecipeFromMetafields(mfs);
    if(r.length) out[`variant:${v.id}`]=r;
  }
  return out;
}



  const okIds=[]; const missIds=[];
  const products = await listAllProducts();
for(const p of products){
  let found=false;

  const scanProductId = PRODUCT_RECIPE_OVERRIDES[String(p.id)] || p.id;

  // читаем у "донорского" продукта
  const pMfs = await listProductMetafields(scanProductId);
  const pRec = pickRecipeFromMetafields(pMfs);
  if(pRec.length){ out[`product:${p.id}`]=pRec; found=true; }

  const vars = await listProductVariants(scanProductId);
  for(const v of vars){
    const mfs = await listVariantMetafields(v.id);
    const r = pickRecipeFromMetafields(mfs);
    if(r.length){ out[`variant:${v.id}`]=r; found=true; }
  }

  if(found) okIds.push(String(p.id)); else missIds.push(String(p.id));
}

  out["__REPORT_OK"]=okIds;
  out["__REPORT_MISS"]=missIds;
  out["__stats"]={ total:products.length, ok:okIds.length, miss:missIds.length };
  return out;
}
