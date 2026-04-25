# Cookies and similar technologies — transparency template

Adapt for your public site / login page cookie banner or privacy centre.

## Strictly necessary (typical for Sparrow admin)

| Name / type | Purpose | Duration |
|-------------|---------|----------|
| Session cookie | Maintains logged-in session after authentication | Browser session or configured lifetime |
| CSRF token cookie | Prevents cross-site request forgery on form POSTs | Session |
| “Remember me” | Optional extended login | As configured (Flask-Login) |

## Optional / analytics

_List only if you enable them (e.g. analytics, chat widgets). Many deployments use none on the admin app._

## Local storage / similar

_Plugins (e.g. website builder) may use `localStorage` for UI state — document per product._

## Control

Users can clear cookies via browser settings. Disabling strictly necessary cookies will prevent login.

---

_Link this document from your privacy notice or cookie banner._
