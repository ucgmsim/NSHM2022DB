CREATE TABLE IF NOT EXISTS fault (
    fault_id INTEGER PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,
    parent_id INTEGER NOT NULL,
    tect_type INT,
    FOREIGN KEY(parent_id) REFERENCES parent_fault(parent_id)
);

CREATE TABLE IF NOT EXISTS parent_fault (
    parent_id INTEGER PRIMARY KEY NOT NULL,
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS fault_plane (
    plane_id INTEGER PRIMARY KEY,
    top_left_lat REAL NOT NULL,
    top_left_lon REAL NOT NULL,
    top_right_lat REAL NOT NULL,
    top_right_lon REAL NOT NULL,
    bottom_right_lat REAL NOT NULL,
    bottom_right_lon REAL NOT NULL,
    bottom_left_lat REAL NOT NULL,
    bottom_left_lon REAL NOT NULL,
    top_depth REAL NOT NULL,
    bottom_depth REAL NOT NULL,
    rake REAL NOT NULL,
    fault_id INTEGER NOT NULL,
    FOREIGN KEY(fault_id) REFERENCES fault(fault_id)
);

CREATE TABLE IF NOT EXISTS rupture (
    rupture_id INTEGER PRIMARY KEY
    -- Maybe I'll add some extra tables here?
);

CREATE TABLE IF NOT EXISTS rupture_faults (
    rupture_fault_id INTEGER PRIMARY KEY,
    rupture_id INTEGER NOT NULL,
    fault_id INTEGER NOT NULL,
    UNIQUE(rupture_id, fault_id)
    FOREIGN KEY(fault_id) REFERENCES fault(fault_id)
    FOREIGN KEY(rupture_id) REFERENCES rupture(rupture_id)
);

CREATE TABLE IF NOT EXISTS magnitude_frequency_distribution (
    entry_id INTEGER PRIMARY KEY,
    fault_id INTEGER NOT NULL,
    magnitude REAL NOT NULL,
    rate REAL NOT NULL,
    UNIQUE(fault_id, magnitude)
    FOREIGN KEY(fault_id) REFERENCES fault(fault_id)
);
