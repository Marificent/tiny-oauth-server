// refresh_and_report.js
// Orquestra: 1) atualiza MVs  2) gera relatório.
// Se qualquer etapa falhar, sai com erro (exit code != 0).

const fs = require("fs");
const { spawnSync } = require("node:child_process");

function assertFile(path) {
  if (!fs.existsSync(path)) {
    console.error(`❌ Arquivo não encontrado: ${path}`);
    process.exit(1);
  }
}

function runNode(script) {
  console.log(`▶️  Rodando ${script}...`);
  const p = spawnSync(process.execPath, [script], { encoding: "utf8" });
  if (p.stdout) process.stdout.write(p.stdout);
  if (p.stderr) process.stderr.write(p.stderr);
  if (p.status !== 0) {
    console.error(`❌ ${script} falhou (exit ${p.status}).`);
    process.exit(p.status || 1);
  }
  console.log(`✅ ${script} ok.\n`);
}

function main() {
  // Sanidade mínima
  assertFile("analytics_refresh.js");
  assertFile("report.js");

  // 1) Atualiza/refresh materialized views
  runNode("analytics_refresh.js");

  // 2) Gera relatório (usa as views)
  runNode("report.js");

  console.log("🎉 refresh_and_report concluído.");
}

main();
