async function transformOrder(order) {
  const after = [];
  const handled = new Set();

  const tags = String(order.tags || "").toLowerCase();
  const tagHint = /simple bundles|bundle|aftersell|upcart|skio/.test(tags);

  const sb = sbDetectFromOrder(order);

  if (!sb && !tagHint) {
    console.log("SKIP ORDER (no bundle detected):", order.id, order.name);
    return { after: [] };
  }

  if (sb) {
    for (const c of sb.children) {
      handled.add(c.id);
      const imageUrl = await getVariantImage(c.variant_id);
      pushLine(after, c, imageUrl);
      console.log("SB CHILD", { id: c.id, title: c.title, variant_id: c.variant_id, imageUrl: imageUrl ? "OK" : "NO" });
    }
    return { after };
  }

  console.log("TAG-ONLY HINT (fallback pass-through):", order.id, order.name, tags);
  for (const li of (order.line_items || [])) {
    if (handled.has(li.id)) continue;
    const imageUrl = await getVariantImage(li.variant_id);
    pushLine(after, li, imageUrl);
  }

  return { after };
}
