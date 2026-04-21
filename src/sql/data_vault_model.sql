-- Data Vault 2.0 — Hub, Link, Satellite pattern

-- Hub: core business key
CREATE TABLE IF NOT EXISTS DV.HUB_CUSTOMER (
    customer_hk         VARCHAR(64)     NOT NULL PRIMARY KEY,
    customer_id         VARCHAR(50)     NOT NULL,
    load_date           TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    record_source       VARCHAR(100)
);

-- Satellite: descriptive attributes
CREATE TABLE IF NOT EXISTS DV.SAT_CUSTOMER_DETAILS (
    customer_hk         VARCHAR(64)     NOT NULL,
    load_date           TIMESTAMP_NTZ   NOT NULL,
    customer_name       VARCHAR(200),
    email               VARCHAR(200),
    segment             VARCHAR(50),
    hash_diff           VARCHAR(64),
    PRIMARY KEY (customer_hk, load_date)
);

-- Link: relationship between entities
CREATE TABLE IF NOT EXISTS DV.LINK_CUSTOMER_ORDER (
    link_hk             VARCHAR(64)     NOT NULL PRIMARY KEY,
    customer_hk         VARCHAR(64)     NOT NULL,
    order_hk            VARCHAR(64)     NOT NULL,
    load_date           TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);
