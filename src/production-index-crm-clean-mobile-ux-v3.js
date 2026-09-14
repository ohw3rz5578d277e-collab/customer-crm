// Legacy compatibility entrypoint.
// Canonical Owner App Shell owns navigation and responsive UI.
// Keep this module as a pass-through so historical import chains cannot re-inject the old Today/mobile navigation or permanent DOM observer.
import app from "./production-index-crm-customer-list-detail-v2.js";

export default app;
