import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import {
  RESERVATION_INTERNAL_PRINCIPAL,
  RESERVATION_HANDOFF_BASE_PATH,
  reservationInternalUser,
  reservationHandoffBasePath,
  reservationBrowserHandoffHealth,
  patchReservationHandoffHtml
} from "../src/crm-reservation-browser-handoff.mjs";

const SECRET = "unit-test-crm-internal-secret";
const env = { CRM_INTERNAL_TOKEN: SECRET };
const request = (headers = {}, suffix = "/admin") => new Request("https://customer-crm.test" + suffix, { headers });
const validHeaders = {
  "x-internal-token": SECRET,
  "x-crm-internal-principal": "reservation-app"
};

test("internal principal requires exact token and exact principal", () => {
  assert.equal(RESERVATION_INTERNAL_PRINCIPAL, "reservation-app");
  assert.equal(reservationInternalUser(request(), env), null);
  assert.equal(reservationInternalUser(request({ "x-crm-internal-principal": "reservation-app" }), env), null);
  assert.equal(reservationInternalUser(request({ "x-internal-token": SECRET }), env), null);
  assert.equal(reservationInternalUser(request({ "x-internal-token": "wrong", "x-crm-internal-principal": "reservation-app" }), env), null);
  assert.equal(reservationInternalUser(request({ "x-internal-token": SECRET, "x-crm-internal-principal": "wrong" }), env), null);
  assert.equal(reservationInternalUser(request(validHeaders), {}), null);
  assert.deepEqual(reservationInternalUser(request(validHeaders), env), {
    email: "reservation-app@internal.invalid",
    role: "admin",
    principal: "reservation_service"
  });
});

test("query token cannot establish internal principal", () => {
  assert.equal(reservationInternalUser(request({}, "/admin?token=" + encodeURIComponent(SECRET)), env), null);
});

test("handoff base path is exact and only available to valid internal principal", () => {
  assert.equal(RESERVATION_HANDOFF_BASE_PATH, "/customer-management");
  assert.equal(reservationHandoffBasePath(request({ ...validHeaders, "x-crm-handoff-base-path": "/customer-management" }), env), "/customer-management");
  assert.equal(reservationHandoffBasePath(request({ ...validHeaders, "x-crm-handoff-base-path": "/customer-management/" }), env), "");
  assert.equal(reservationHandoffBasePath(request({ ...validHeaders, "x-crm-handoff-base-path": "https://evil.example" }), env), "");
  assert.equal(reservationHandoffBasePath(request({ "x-crm-handoff-base-path": "/customer-management" }), env), "");
});

test("internal HTML bootstrap rewrites only the fixed same-origin API namespace", () => {
  const direct = '<!doctype html><html><head><title>CRM</title></head><body><a href="/admin">Home</a><script>fetch("/api/customers")</script></body></html>';
  assert.equal(patchReservationHandoffHtml(direct, ""), direct);
  const internal = patchReservationHandoffHtml(direct, "/customer-management");
  assert.match(internal, /crm-reservation-handoff-20260822/);
  assert.match(internal, /window\.__CRM_BASE_PATH__=BASE/);
  assert.match(internal, /value==='\/api'/);
  assert.match(internal, /value\.indexOf\('\/api\/'\)===0/);
  assert.match(internal, /a\[href="\/admin"\]/);
  assert.ok(!internal.includes(SECRET));
  assert.ok(!internal.includes("https://evil.example"));
});

test("health markers are secret-free and preserve direct Access protection", () => {
  const health = reservationBrowserHandoffHealth();
  assert.equal(health.reservation_browser_handoff_contract, true);
  assert.equal(health.reservation_browser_handoff_internal_principal, true);
  assert.equal(health.reservation_browser_handoff_principal, "reservation-app");
  assert.equal(health.reservation_browser_handoff_base_path, "/customer-management");
  assert.equal(health.direct_cloudflare_access_protection, true);
  assert.ok(!JSON.stringify(health).includes(SECRET));
});

test("secure entry keeps Cloudflare Access and adds internal principal before Access lookup", () => {
  const src = readFileSync(new URL("../src/secure-index.js", import.meta.url), "utf8");
  assert.match(src, /import \{ reservationInternalUser \} from "\.\/crm-reservation-browser-handoff\.mjs";/);
  const currentUser = src.slice(src.indexOf("async function getCurrentUser"), src.indexOf("function isManager"));
  assert.ok(currentUser.indexOf("reservationInternalUser(request, env)") >= 0);
  assert.ok(currentUser.indexOf("reservationInternalUser(request, env)") < currentUser.indexOf("accessEmail(request)"));
  assert.match(src, /cf-access-authenticated-user-email/);
  assert.match(src, /cf-access-user-email/);
  assert.match(src, /hasUrlToken\(url\)/);
  assert.match(src, /Google login through Cloudflare Access is required/);
});

test("browser root preserves P0 identity handlers and gates handoff HTML separately", () => {
  const src = readFileSync(new URL("../src/production-index-crm-browser-root-entry.js", import.meta.url), "utf8");
  assert.match(src, /handleCanonicalLineFollow/);
  assert.match(src, /handleGuardedCustomerUpsert/);
  assert.match(src, /handleIdentityDamageDiagnostic/);
  assert.match(src, /handleCustomerIdentityResolver/);
  assert.match(src, /reservationInternalUser/);
  assert.match(src, /reservationHandoffBasePath/);
  assert.match(src, /patchReservationHandoffHtml/);
  assert.match(src, /invalid_reservation_handoff_base_path/);
  assert.match(src, /if\(!internal\) return injectReviewLink\(response, url\)/);
  assert.match(src, /x-crm-reservation-handoff/);
});

test("handoff helper contains no client-side secret storage or Access-header impersonation", () => {
  const src = readFileSync(new URL("../src/crm-reservation-browser-handoff.mjs", import.meta.url), "utf8");
  assert.doesNotMatch(src, /localStorage/);
  assert.doesNotMatch(src, /sessionStorage/);
  assert.doesNotMatch(src, /console\./);
  assert.doesNotMatch(src, /cf-access-authenticated-user-email/i);
  assert.doesNotMatch(src, /cf-access-user-email/i);
  assert.doesNotMatch(src, /searchParams\.get\(["']token["']\)/);
});
