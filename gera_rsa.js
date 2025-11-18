const { generateKeyPairSync } = require('crypto');
const { privateKey, publicKey } = generateKeyPairSync('rsa', {
  modulusLength: 2048,
  publicKeyEncoding: { type: 'spki', format: 'pem' },
  privateKeyEncoding: { type: 'pkcs8', format: 'pem' },
});
console.log("PRIVATE KEY ↓↓↓");
console.log(privateKey);
console.log("PUBLIC KEY ↓↓↓");
console.log(publicKey);
