import fs from "fs";

const path = "src/index.js";
let s = fs.readFileSync(path, "utf8");

s = s.replace(
  /const BUILD = "customer-crm-api-complete-[^"]+";/,
  'const BUILD = "customer-crm-api-complete-20260530-secrets-required-01";'
);

s = s.replace(
  /\/\/ build: customer-crm-api-complete-[^\n]+/,
  "// build: customer-crm-api-complete-20260530-secrets-required-01"
);

s = s.replace(/const DEFAULT_ADMIN_TOKEN = "[^"]*";\n/, "");
s = s.replace(/const DEFAULT_INTERNAL_TOKEN = "[^"]*";\n/, "");

s = s.replace(
  /function getAdminToken\(env\) \{\n\s*return text\(env\.ADMIN_TOKEN\) \|\| [A-Z_]+;\n\}/,
  `function getAdminToken(env) {
  return text(env.ADMIN_TOKEN);
}

function requireSecret(env, name) {
  const value = text(env && env[name]);
  if (!value) throw new Error(name + " is not configured");
  return value;
}`
);

s = s.replace(
  /const adminToken = getAdminToken\(env\);\n\s*const internalToken =\n\s*text\(env\.LINE_INTERNAL_TOKEN\) \|\|\n\s*text\(env\.LINE_WORKER_INTERNAL_TOKEN\) \|\|\n\s*text\(env\.RESERVATION_INTERNAL_TOKEN\) \|\|\n\s*[A-Z_]+;/,
  `const adminToken = requireSecret(env, "ADMIN_TOKEN");
  const internalToken =
    text(env.LINE_INTERNAL_TOKEN) ||
    text(env.LINE_WORKER_INTERNAL_TOKEN) ||
    text(env.RESERVATION_INTERNAL_TOKEN);
  if (!internalToken) {
    return {
      ok: false,
      connected: false,
      message: "LINE internal token is not configured",
      messages: [],
      debug: [{ step: "missing_line_internal_token" }]
    };
  }`
);

s = s.replace(
  /\n\s*\/\/ line-webhook-worker[^\n]*\n\s*url\.searchParams\.set\("token", [A-Z_]+\);\n/g,
  "\n"
);

s = s.replace(/"x-admin-token": [A-Z_]+,/g, '"x-admin-token": adminToken,');

s = s.replace(
  /if \(url\.pathname === "\/" \|\| url\.pathname === "\/health"\) \{\n\s*await ensureSchema\(env\.DB\);\n\s*return json\(\{ ok: true, service: "customer-crm-api", build: BUILD, time: nowIso\(\), hasDb: !!env\.DB, admin_url: url\.origin \+ "\/admin\?token=" \+ getAdminToken\(env\) \}\);\n\s*\}/,
  `if (url.pathname === "/" || url.pathname === "/health") {
        await ensureSchema(env.DB);
        return json({ ok: true, service: "customer-crm-api", build: BUILD, time: nowIso(), hasDb: !!env.DB, secure: true });
      }`
);

s = s.replace(
  /return html\(`<div style="font-family:sans-serif;padding:24px"><h1>Unauthorized<\/h1><p>管理画面を開くには token が必要です。<\/p><p><a href="\/admin\?token=\$\{getAdminToken\(env\)\}">\/admin\?token=\$\{getAdminToken\(env\)\}<\/a><\/p><\/div>`, 401\);/,
  `return html(\`<div style="font-family:sans-serif;padding:24px"><h1>Unauthorized</h1><p>Googleログインまたは管理者認証が必要です。</p></div>\`, 401);`
);

const forbidden = [
  /const DEFAULT_ADMIN_TOKEN/,
  /const DEFAULT_INTERNAL_TOKEN/,
  /searchParams\.set\("token"/,
  /admin_url:/,
  /\/admin\?token=/
];

for (const re of forbidden) {
  if (re.test(s)) {
    console.error("NG: forbidden pattern still exists:", re);
    process.exit(1);
  }
}

fs.writeFileSync(path, s);
console.log("OK: src/index.js hardened");
