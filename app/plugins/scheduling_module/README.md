# Scheduling & Shifts module

Sling-level scheduling: shifts, availability, time off, open shifts, templates, labour cost, audit, time clock with optional geofencing, and exportable timesheets. Integrates with the Employee Portal cluster.

## Install / upgrade

**Prerequisites:** Install `time_billing_module` first (provides tb_contractors, clients, sites, job_types).

From repo root:

```bash
python app/plugins/scheduling_module/install.py install
```

To apply new tables or columns after an update (idempotent):

```bash
python app/plugins/scheduling_module/install.py upgrade
```

Uninstall (drops all scheduling tables; irreversible with `--drop-data`):

```bash
python app/plugins/scheduling_module/install.py uninstall --drop-data
```

## Integration

See [INTEGRATION.md](INTEGRATION.md) for how this module works with the employee portal, time_billing, and work modules (session, messages, links, install order).

## Features

- **Admin:** Week/month views, shift CRUD, templates (clone, apply, save week as template), open shift claims, job type requirements, labour & budget settings, audit log, announcements, shift tasks, clock locations (geofence), export timesheets.
- **Staff (portal):** My day, clock in/out, open shifts (claim, eligible filter), time off, availability, swap requests, shift tasks (list/complete via API).
