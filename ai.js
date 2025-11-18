// ai.js
require("dotenv").config();
const OpenAI = require("openai");

const client = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
});

async function explainTinyData(question, payload) {
  const response = await client.responses.create({
    model: "gpt-4o-mini",
    instructions: `
      Você é um analista especializado em e-commerce, vendas, plumas e itens decorativos.
      Responda SEMPRE em português do Brasil.
      Analise APENAS os dados enviados.
      Se os dados forem poucos ou vazios, explique isso de forma clara.
      Quando houver dados:
      - gere um resumo executivo,
      - mostre totais (faturamento, itens, pedidos),
      - analise sazonalidade,
      - destaque padrões importantes.
    `,
    input: [
      `Pergunta do usuário: ${question}`,
      "A seguir estão os dados que foram obtidos do Tiny:",
      JSON.stringify(payload).slice(0, 12000)
    ]
  });

  return response.output_text; 
}

module.exports = { explainTinyData };
