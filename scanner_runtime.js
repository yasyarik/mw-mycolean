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
  ["bundles","components_json"],
  ["simple_bundles","bundled_variants"]
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
    const qty = Number(it.quantity || it.qty || it.qty_each || it.count || it.quantity_in_bundle || 0);
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

function productMfAliases(){
  return NS_KEYS.map(([ns,key],i)=>`p_mf_${i}: metafield(namespace:"${ns}", key:"${key}"){ value }`).join('\n');
}
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
    const pRaw = firstNonEmptyMetafield(p, "p_mf_");
    const pArr = parseRecipeArray(pRaw);
    if(pArr.length) map[`product:${pid}`] = pArr;

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

async function restJson(path){
  const shop  = process.env.SHOPIFY_SHOP;
  const token = process.env.SHOPIFY_ADMIN_ACCESS_TOKEN;
  const r = await fetch(`https://${shop}/admin/api/2025-01${path}`,{
    headers:{ "X-Shopify-Access-Token": token, "Content-Type":"application/json" }
  });
  if(!r.ok){
    const t=await r.text().catch(()=> "");
    throw new Error(`REST ${path} -> HTTP ${r.status} ${r.statusText} ${t}`);
  }
  return r.json();
}

export async function dumpMeta(productId){
  const pid = String(productId);
  const out = { productId: pid, product: [], variants: {} };

  const query = `
    query DumpMeta($pid:ID!){
      product(id:$pid){
        id
        metafields(first: 200){
          edges{ node{ namespace key type value } }
        }
        variants(first: 200){
          edges{
            node{
              id
              metafields(first: 200){
                edges{ node{ namespace key type value } }
              }
            }
          }
        }
      }
    }
  `;
  try{
    const data = await gqlFetch(gqlBody(query,{ pid: `gid://shopify/Product/${pid}` }));
    const prod = data?.data?.product;
    if(prod){
      const pEdges = prod?.metafields?.edges || [];
      out.product = pEdges.map(e=>e.node);
      const vEdges = prod?.variants?.edges || [];
      for(const ve of vEdges){
        const vid = String(ve.node.id).replace(/^gid:\/\/shopify\/ProductVariant\//,"");
        const mEdges = ve?.node?.metafields?.edges || [];
        out.variants[vid] = mEdges.map(e=>e.node);
      }
      return { ok:true, via:"graphql", ...out };
    }
  }catch(e){}

  try{
    const p = await restJson(`/products/${pid}.json`);
    const variants = Array.isArray(p?.product?.variants) ? p.product.variants : [];
    const pMeta = await restJson(`/products/${pid}/metafields.json`);
    out.product = Array.isArray(pMeta?.metafields) ? pMeta.metafields.map(m=>({
      namespace: m.namespace, key: m.key, type: m.type || m.value_type || "", value: String(m.value ?? "")
    })) : [];

    for(const v of variants){
      const vid = v.id;
      try{
        const vm = await restJson(`/variants/${vid}/metafields.json`);
        out.variants[String(vid)] = Array.isArray(vm?.metafields) ? vm.metafields.map(m=>({
          namespace: m.namespace, key: m.key, type: m.type || m.value_type || "", value: String(m.value ?? "")
        })) : [];
      }catch(_){}
    }
    return { ok:true, via:"rest", ...out };
  }catch(e){
    return { ok:false, error: String(e.message||e), ...out };
  }
}
