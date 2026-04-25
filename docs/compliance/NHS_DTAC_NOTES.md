# NHS DTAC (Digital Technology Assessment Criteria) — alignment notes

DTAC is **organisation- and product-specific**. This file lists areas assessors often examine; map each to **your** evidence pack.

| Criterion area | Typical evidence | Sparrow-related technical notes |
|----------------|------------------|--------------------------------|
| **Clinical safety** | DCB0129 / DCB0160 where applicable | See **[DCB0129_DCB0160_GUIDE.md](./DCB0129_DCB0160_GUIDE.md)** — supplier (0129) vs deployer (0160); hazard log / safety case live **mostly outside** raw git |
| **Data protection** | DPIA, ROPA, privacy notice, DPAs | Use templates in this folder + configured deployment |
| **Technical security** | Pen test, secure SDLC, patching | CSRF, sessions, JWT, rate limits, audit log, ZIP handling |
| **Interoperability** | Standards used (e.g. APIs, coding) | Document your integrations per deployment |
| **Usability / accessibility** | WCAG claims | Assess actual UI you ship |

Complete the official DTAC workbook for your NHS customer or internal governance.
