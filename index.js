import express from "express";
import dotenv from "dotenv";
import getRawBody from "raw-body";
import crypto from "crypto";

dotenv.config();
const app = express();

console.log("MW UP", "secret_len", (process.env.SHOPIFY_WEBHOOK_SECRET || "").length);

function hmacOk(raw, header, secret) {
  if (!header) return false;
  const sig = crypto.createHmac("sha256", secret).update(raw).digest("base64");
  const a = Buffer.from(header);
  const b = Buffer.from(sig);
  if (a.length !== b.length) return false;
  return crypto.timingSafeEqual(a, b);
}

function pickMark(order) {
  const note = String(order.note || "");
  const mT = note.match(/__MW_THEME\s*=\s*([^\s;]+)/i);
  const mD = note.match(/__MW_DEBUG\s*=\s*([^\s;]+)/i);
  if (mT) return { theme: mT[1], debug: !!(mD && mD[1].toLowerCase() === "on") };
  const attrs = Object.fromEntries((order.attributes || []).map(a => [a.name, a.value]));
  if (attrs.__MW_THEME) return { theme: attrs.__MW_THEME, debug: attrs.__MW_DEBUG === "on" };
  return null;
}

function toNum(x) {
  const n = Number.parseFloat(String(x || "0"));
  return Number.isFinite(n) ? n : 0;
}

function isBundleParent(li) {
  const p = li.properties || [];
  if (p.find(x => x.name === "_sb_parent" && String(x.value).toLowerCase() === "true")) return true;
  if (p.find(x => x.name === "_bundle" || x.name === "bundle_id")) return true;
  return false;
}

function bundleKey(li) {
  const p = li.properties || [];
  const f1 = p.find(x => ["_sb_bundle_id", "bundle_id", "_bundle_id"].includes(x.name));
  if (f1) return String(f1.value);
  const f2 = p.find(x => x.name === "_sb_key" || x.name === "bundle_key");
  return f2 ? String(f2.value) : null;
}

function transformOrder(order) {
  const before = order.line_items.map(li => ({
    id: li.id, title: li.title, sku: li.sku || null, qty: li.quantity,
    price: toNum(li.price), total: toNum(li.price) * li.quantity,
    key: bundleKey(li), parent: isBundleParent(li)
  }));

  const groups = {};
  for (const li of order.line_items) {
    const k = bundleKey(li);
    if (!k) continue;
    if (!groups[k]) groups[k] = { parent: null, children: [] };
    if (isBundleParent(li)) groups[k].parent = li; else groups[k].children.push(li);
  }

  const after = [];
  const handled = new Set();

  for (const [k, g] of Object.entries(groups)) {
    const parent = g.parent;
    const kids = g.children;
    if (parent) handled.add(parent.id);
    for (const c of kids) handled.add(c.id);

    if (parent && kids.length > 0) {
      const parentTotal = toNum(parent.price) * parent.quantity;
      let qtySum = 0;
      for (const c of kids) qtySum += c.quantity;

      after.push({
        id: parent.id, title: parent.title, sku: parent.sku || null, qty: parent.quantity,
        unitPrice: 0, parent: true, key: k
      });

      if (parentTotal > 0 && qtySum > 0) {
        let rest = Math.round(parentTotal * 100);
        for (let i = 0; i < kids.length; i++) {
          const c = kids[i];
          const share = i === kids.length - 1
            ? rest
            : Math.round((parentTotal * (c.quantity / qtySum)) * 100);
          rest -= share;
          const unit = c.quantity > 0 ? share / c.quantity / 100 : 0;
          after.push({
            id: c.id, title: c.title, sku: c.sku || null, qty: c.quantity,
            unitPrice: Number(unit.toFixed(2)), parent: false, key: k
          });
        }
      } else {
        for (const c of kids) {
          after.push({
            id: c.id, title: c.title, sku: c.sku || null, qty: c.quantity,
            unitPrice: toNum(c.price), parent: false, key: k
          });
        }
      }
    }
  }

  for (const li of order.line_items) {
    if (handled.has(li.id)) continue;
    after.push({
      id: li.id, title: li.title, sku: li.sku || null, qty: li.quantity,
      unitPrice: toNum(li.price), parent: false, key: bundleKey(li)
    });
  }

  return { before, after };
}

const last = [];
function remember(entry) {
  last.push(entry);
  while (last.length > 10) last.shift();
}

app.post("/webhooks/orders-create", async (req, res) => {
  const raw = await getRawBody(req);
  const hdr = req.headers["x-shopify-hmac-sha256"] || "";
  const ok = hdr && hmacOk(raw, hdr, process.env.SHOPIFY_WEBHOOK_SECRET || "");
  if (!ok) { res.status(401).send("bad hmac"); return; }

  const order = JSON.parse(raw.toString("utf8"));
  const mark = pickMark(order);
  if (!mark || !String(mark.theme || "").startsWith("preview-")) {
    console.log("skip", order.id, order.name);
    res.status(200).send("skip");
    return;
  }

  const conv = transformOrder(order);
  remember({
    id: order.id,
    name: order.name,
    theme: mark.theme,
    debug: mark.debug,
    currency: order.currency,
    total_price: order.total_price,
    payload: conv
  });

  console.log("MATCH", order.id, order.name, mark, "items:", conv.after.length);
  res.status(200).send("ok");
});

app.get("/debug/last", (req, res) => {
  res.json(last[last.length - 1] || null);
});

app.post("/test", express.text({ type: "*/*" }), (req, res) => {
  console.log("TEST HIT", new Date().toISOString(), req.headers["user-agent"] || "");
  res.send("ok");
});

app.get("/health", (req, res) => res.send("ok"));
app.listen(process.env.PORT || 8080);
