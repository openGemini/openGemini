-- Tests for SQLite

-- enable foreign key
PRAGMA foreign_keys = ON;

-- create table for database
CREATE TABLE IF NOT EXISTS dbs (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    name    VARCHAR NOT NULL,
    CONSTRAINT db_name_unique
        UNIQUE (name)
);

-- create table for retention policies
CREATE TABLE IF NOT EXISTS rps (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    db_id   INTEGER NOT NULL,
    name    VARCHAR NOT NULL,
    CONSTRAINT fk_dbs
        FOREIGN KEY (db_id)
        REFERENCES dbs(id)
        ON DELETE CASCADE
    CONSTRAINT rp_name_unique
        UNIQUE (db_id, name)
);

-- create table for measurements
CREATE TABLE IF NOT EXISTS msts (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    rp_id   INTEGER NOT NULL,
    name    VARCHAR NOT NULL,
    CONSTRAINT fk_rps
        FOREIGN KEY (rp_id)
        REFERENCES rps(id)
        ON DELETE CASCADE
    CONSTRAINT mst_name_unique
        UNIQUE (rp_id, name)
);

-- -- create table for files
CREATE TABLE IF NOT EXISTS files (
    sequence    INTEGER,
    level       INTEGER,
    merge       INTEGER,
    extent      INTEGER,
    mst_id      INTEGER,
    CONSTRAINT file_name_unique
        UNIQUE(mst_id, sequence, level, extent, merge)
);
