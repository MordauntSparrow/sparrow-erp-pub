-- Link runsheets to Medical Records ``cura_operational_events`` (expected roster, name, location).
-- Optional per-crew ``fleet_vehicle_id`` (Fleet module ``fleet_vehicles.id``). No FKs so Time Billing
-- upgrades cleanly when Medical Records / Fleet are absent.

ALTER TABLE runsheets
    ADD COLUMN cura_operational_event_id BIGINT NULL DEFAULT NULL
        COMMENT 'Logical link to cura_operational_events.id (Medical Records / Cura ops hub)';

CREATE INDEX idx_runsheets_cura_event ON runsheets (cura_operational_event_id);

ALTER TABLE runsheet_assignments
    ADD COLUMN fleet_vehicle_id BIGINT NULL DEFAULT NULL
        COMMENT 'Optional fleet_vehicles.id for per-crew vehicle allocation';

CREATE INDEX idx_rsa_fleet_vehicle ON runsheet_assignments (fleet_vehicle_id);
