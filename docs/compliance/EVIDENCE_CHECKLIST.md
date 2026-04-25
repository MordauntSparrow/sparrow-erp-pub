# Evidence checklist (pre-accreditation / DSPT-style)

Tick and attach evidence (screenshots, configs, minutes, signed policies) as your organisation requires.

## Governance

- [ ] Named information governance lead / DPO contact (where required)
- [ ] Information security policy approved and dated
- [ ] Acceptable use policy for staff
- [ ] Change management record for production deployments

## Legal / transparency

- [ ] Privacy notice published and linked from login/public entry points (if applicable)
- [ ] Cookie / similar technologies description (see `COOKIE_DESCRIPTION.md`)
- [ ] ROPA completed (`ROPA_TEMPLATE.md`)
- [ ] DPIA for special-category / high-risk processing (`DPIA_SCAFFOLD.md`) — **especially** if processing health data

## Technical (this deployment)

- [ ] `SECRET_KEY` and (recommended) `JWT_SECRET_KEY` set; not default
- [ ] `FLASK_ENV=production` or equivalent host signal where you rely on prod-only defaults
- [ ] HTTPS enforced; `SESSION_COOKIE_SECURE=true` (or edge equivalent)
- [ ] `REDIS_URL` / `RATELIMIT_STORAGE_URI` for distributed rate limiting in multi-instance setups
- [ ] Database credentials rotated and not shared
- [ ] Backups: encrypted at rest off-site; restore tested; access restricted
- [ ] `app/logs/audit.log` protected; retention defined; reviewed periodically
- [ ] Subprocessors listed with DPAs/SCCs (`SUBPROCESSORS_REGISTER.md`)

## Operations

- [ ] Incident response plan tested (`INCIDENT_RESPONSE.md`)
- [ ] Staff training records (IG / phishing / secure handling)
- [ ] Offboarding checklist (revoke accounts, tokens, keys)

## Assurance

- [ ] Vulnerability management process (dependency updates)
- [ ] Penetration test or security assessment (scope: admin, APIs, plugins in use)
- [ ] Supplier assurance for hosting (e.g. platform SOC reports if used)
