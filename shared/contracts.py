"""Small shared contracts used by generator and validation scripts."""

REQUIRED_READING_COLUMNS = frozenset({
    "reading_id",
    "event_timestamp",
    "meter_id",
    "transformer_id",
    "voltage_v",
    "active_power_kw",
    "reactive_power_kvar",
    "energy_kwh",
    "power_factor",
    "schema_version",
})

CUSTOMER_PROFILES = {
    "RESIDENTIAL": (0.35, 1.8),
    "COMMERCIAL": (1.2, 4.0),
    "INDUSTRIAL": (3.0, 12.0),
    "AGRICULTURAL": (0.8, 5.0),
    "GOVERNMENT": (0.7, 3.5),
    "PUBLIC_LIGHTING": (0.1, 1.5),
    "OTHER": (0.3, 2.0),
}
