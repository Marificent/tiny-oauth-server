require("dotenv").config();
const fs = require("fs");
const path = require("path");

const TOKEN   = process.env.WHATSAPP_TOKEN;
const PHONE_ID= process.env.WHATSAPP_PHONE_NUMBER_ID;
const TO      = process.env.WHATSAPP_RECIPIENT || "+5511987820087";

if (!TOKEN || !PHONE_ID) {
  console.error("❌ Configure WHATSAPP_TOKEN e WHATSAPP_PHONE_NUMBER_ID no .env");
  process.exit(1);
}

function latestLog() {
  const dir = path.join(__dirname,"logs");
  if (!fs.existsSync(dir)) return null;
  const files = fs.readdirSync(dir).filter(f=>f.endsWith(".txt"))
    .map(f=>({f, t: fs.statSync(path.join(dir,f)).mtimeMs}))
    .sort((a,b)=>b.t-a.t);
  if (!files.length) return null;
  const full = path.join(dir, files[0].f);
  let txt = fs.readFileSync(full,"utf8");
  if (txt.length>3500) txt = txt.slice(0,3500) + "\n\n[…truncado…]";
  return {name: files[0].f, txt};
}

async function send(text) {
  const url = `https://graph.facebook.com/v20.0/${PHONE_ID}/messages`;
  const payload = {
    messaging_product: "whatsapp",
    to: TO,
    type: "text",
    text: { body: text }
  };
  const res = await fetch(url, {
    method: "POST",
    headers: { "Authorization": `Bearer ${TOKEN}`, "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const body = await res.text();
  if (!res.ok) throw new Error(`HTTP ${res.status} - ${body}`);
  console.log("✅ Enviado. Resposta:", body);
}

(async()=>{
  try {
    const L = latestLog();
    const text = L
      ? `📤 IA Plumas • Relatório\nArquivo: ${L.name}\n\n${L.txt}`
      : "ℹ️ Não encontrei log em C:\\Users\\stefa\\tiny-oauth\\logs\\ (rode: node refresh_and_report.js)";
    await send(text);
  } catch(e) {
    console.error("❌ Falha:", e.message || e);
    process.exit(1);
  }
})();
