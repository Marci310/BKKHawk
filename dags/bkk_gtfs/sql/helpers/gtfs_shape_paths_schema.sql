CREATE TABLE IF NOT EXISTS gtfs_shape_paths (
    shape_path_id_pk SERIAL PRIMARY KEY,
    shape_id VARCHAR(50) NOT NULL,
    shape_pt_sequence INT NOT NULL,
    shape_pt_lat FLOAT NOT NULL,
    shape_pt_lon FLOAT NOT NULL,
    shape_dist_traveled FLOAT,
    UNIQUE (shape_id, shape_pt_sequence),
    created_at TIMESTAMP DEFAULT NOW()
);