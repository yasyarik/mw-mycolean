import express from "express";
import dotenv from "dotenv";
import getRawBody from "raw-body";
import crypto from "crypto";

dotenv.config();
const app = express();

app.post("/test", express.text({ type: "*/*" }), (req, res) => {
  console.log("TEST HIT", new Date().toISOString(), req.headers["user-agent"] || "");
  res.send("ok");
});

function hmacOk(raw, header, secret){
  if(!header) return false;
  const sig = crypto.createHmac("sha256", secret).update(raw).digest("base64");
  const a = Buffer.from(header);
  const b = Buffer.from(sig);
  if (a.length !== b.length) return false;
  return crypto.timingSafeEqual(a,b);
}

function pickMark(order){
  const note = String(order.note||"");
  const mT = note.match(/__MW_THEME\s*=\s*([^\s;]+)/i);
  const mD = note.match(/__MW_DEBUG\s*=\s*([^\s;]+)/i);
  if (mT) return { theme:mT[1], debug: !!(mD && mD[1].toLowerCase()==="on") };
  const attrs = Object.fromEntries((order.attributes||[]).map(a=>[a.name,a.value]));
  if (attrs.__MW_THEME) return { theme: attrs.__MW_THEME, debug: attrs.__MW_DEBUG==="on" };
  return null;
}

app.post("/webhooks/orders-create", async (req,res)=>{
  const raw = await getRawBody(req);
  const ok = hmacOk(raw, req.headers["x-shopify-hmac-sha256"], process.env.SHOPIFY_WEBHOOK_SECRET||"");
  if(!ok){ res.status(401).send("bad hmac"); return; }
  const order = JSON.parse(raw.toString("utf8"));
  const mark = pickMark(order);
  if (!mark || !String(mark.theme||"").startsWith("preview-")) {
    console.log("skip", order.id, order.name);
    res.status(200).send("skip"); return;
  }
  console.log("MATCH", order.id, order.name, mark);
  res.status(200).send("ok");
});

app.get("/health", (req,res)=>res.send("ok"));
app.listen(process.env.PORT||8080);
