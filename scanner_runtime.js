// scanner_runtime.js — совместимый с Admin GraphQL без HasMetafieldsIdentifier

const NS_KEYS = [
  ["simple_bundles_2_0","components"],
  ["simple_bundles","components"],
  ["simplebundles","components"],
  ["bundles","components"],
  ["sb","components"],
  ["simple_bundles_2_0","bundle_components"],
  ["simple_bundles","bundle_components"],
  ["bundles","bundle_components"],
  ["simple_bundles_2_0","components_json"],
  ["simple_bundles","components_json"],
  ["bundles","components_json"]
];

function parseRecipeArray(raw){
  if(!raw) return [];
  let val = raw;
  if(typeof val==="string"){
    const s=val.trim();
    if((s.startsWith("[")&&s.endsWith("]")) || (s.startsWith("{")&&s.endsWith("}"))){
      try{ val = JSON.parse(s); }catch{}
    }
  }
  const list = Array.isArray(val) ? val : (val && Array.isArray(val.components) ? val.components : []);
  const out=[];
  for(const it of list){
    if(!it) continue;
    const vid = String(it.variantId || it.variant_id || "").replace(/^gid:\/\/shopify\/ProductVariant\//,"");
    const qty = Number(it.quantity || it.qty || it.qty_each || it.count || 0);
    if(vid && qty>0) out.push({ variantId: vid, qty });
  }
  return out;
}

function gqlBody(query, variables){ return JSON.stringify({ query, variables }); }

async function gqlFetch(body){
  const shop  = process.env.SHOPIFY_SHOP;
  const token = process.env.SHOPIFY_ADMIN_ACCESS_TOKEN;
  const r = await fetch(`https://${shop}/admin/api/2025-01/graphql.json`,{
    method:"POST",
    headers:{ "X-Shopify-Access-Token": token, "Content-Type":"application/json" },
    body
  });
  if(!r.ok){
    const t=await r.text().catch(()=> "");
    throw new Error(`GraphQL HTTP ${r.status} ${r.statusText} ${t}`);
  }
  const j = await r.json();
  if(j.errors) throw new Error(`GraphQL errors: ${JSON.stringify(j.errors).slice(0,500)}`);
  return j;
}

// собираем блок полей для продукта по alias’ам
function productMfAliases(){
  // p_nsX_keyY: metafield(namespace:"...", key:"..."){ value }
  return NS_KEYS.map(([ns,key],i)=>`p_mf_${i}: metafield(namespace:"${ns}", key:"${key}"){ value }`).join('\n');
}
// для варианта
function variantMfAliases(){
  return NS_KEYS.map(([ns,key],i)=>`v_mf_${i}: metafield(namespace:"${ns}", key:"${key}"){ value }`).join('\n');
}

function firstNonEmptyMetafield(obj, prefix){
  for(let i=0;i<NS_KEYS.length;i++){
    const mf = obj?.[`${prefix}${i}`];
    if(mf && mf.value) return mf.value;
  }
  return null;
}

export async function buildBundleMap({ onlyProductId=null } = {}){
  const prodMf = productMfAliases();
  const varMf  = variantMfAliases();

  const query = `
    query BundleScan($first:Int!, $after:String) {
      products(first:$first, after:$after${onlyProductId?`, query:"id:${onlyProductId}"`:""}) {
        edges {
          cursor
          node {
            id
            ${prodMf}
            variants(first: 100) {
              edges {
                node {
                  id
                  sku
                  ${varMf}
                }
              }
            }
          }
        }
        pageInfo { hasNextPage endCursor }
      }
    }
  `;

  const all=[];
  let after=null;
  do{
    const data = await gqlFetch(gqlBody(query,{ first:50, after }));
    const edges = data?.data?.products?.edges || [];
    for(const e of edges) all.push(e.node);
    const pi = data?.data?.products?.pageInfo || {};
    after = pi?.hasNextPage ? pi.endCursor : null;
  }while(after);

  const map = {};
  for(const p of all){
    const pid = String(p.id).replace(/^gid:\/\/shopify\/Product\//,"");

    // 1) пробуем рецепт на уровне продукта
    const pRaw = firstNonEmptyMetafield(p, "p_mf_");
    const pArr = parseRecipeArray(pRaw);
    if(pArr.length) map[`product:${pid}`] = pArr;

    // 2) пробуем рецепты на уровне вариантов
    const vEdges = p?.variants?.edges || [];
    for(const ve of vEdges){
      const v = ve.node;
      const vid = String(v.id).replace(/^gid:\/\/shopify\/ProductVariant\//,"");
      const vRaw = firstNonEmptyMetafield(v, "v_mf_");
      const vArr = parseRecipeArray(vRaw);
      if(vArr.length) map[`variant:${vid}`] = vArr;
    }
  }

  return map;
}
