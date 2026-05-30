import fs from "fs";

function patchIndex() {
  const path = "src/index.js";
  let s = fs.readFileSync(path, "utf8");

  const before = `return html(\`<div style="font-family:sans-serif;padding:24px"><h1>Unauthorized</h1><p>管理画面を開くには token が必要です。</p><p><a href="/admin?token=\${getAdminToken(env)}">/admin?token=\${getAdminToken(env)}</a></p></div>\`, 401);`;

  const after = `return html(\`<div style="font-family:sans-serif;padding:24px"><h1>Unauthorized</h1><p>Googleログインまたは管理者認証が必要です。</p></div>\`, 401);`;

  if (s.includes(before)) {
    s = s.replace(before, after);
  } else {
    s = s.replace(
      /return html\(`<div style="font-family:sans-serif;padding:24px"><h1>Unauthorized<\/h1><p>管理画面を開くには token が必要です。<\/p><p><a href="\/admin\?token=\$\{getAdminToken\(env\)\}">\/admin\?token=\$\{getAdminToken\(env\)\}<\/a><\/p><\/div>`, 401\);/g,
      after
    );
  }

  fs.writeFileSync(path, s);
}

function patchSecureIndex() {
  const path = "src/secure-index.js";
  let s = fs.readFileSync(path, "utf8");

  // secure-index.js 内で旧 index.js を呼ぶためにURLへ token を付けていた処理を撤去。
  // 認証は x-admin-token / Authorization ヘッダーだけで通す。
  s = s.replace(/\n\s*url\.searchParams\.set\("token", token\);/g, "");

  fs.writeFileSync(path, s);
}

patchIndex();
patchSecureIndex();

const checkFiles = ["src/index.js", "src/secure-index.js"];
const forbidden = [
  /\/admin\?token=/,
  /searchParams\.set\("token"/,
  /管理画面を開くには token が必要です/
];

let ng = false;

for (const file of checkFiles) {
  const s = fs.readFileSync(file, "utf8");
  for (const re of forbidden) {
    if (re.test(s)) {
      console.error(`NG: ${file} still has ${re}`);
      ng = true;
    }
  }
}

if (ng) process.exit(1);

console.log("OK: URL token references removed from index.js and secure-index.js");
