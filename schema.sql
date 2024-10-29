-- Create enumerated type for family
CREATE TYPE Family AS ENUM (
    'Wikipedia', 'Wiktionary', 'Wikinews', 'Wikivoyage', 
    'Wikiquote', 'Wikiversity', 'Wikisource', 'Wikibooks', 
    'Wikimedia Commons', 'Wikidata', 'Wikispecies', 'Incubator'
);

-- Create wikis table
CREATE TABLE wikis (
    id SERIAL PRIMARY KEY,
    domain VARCHAR(255),
    family Family
);

-- Create history table
CREATE TABLE history (
    record_md5 VARCHAR(32),
    revision_id INT,
    revision_timestamp TIMESTAMP,
    PRIMARY KEY (record_md5, revision_id)
);

-- Create wikireferences table
CREATE TABLE wikireferences (
    wiki INT,
    page_id INT,
    reference_raw TEXT,
    reference_normalized TEXT,
    reference_raw_md5 CHAR(32),
    reference_md5 CHAR(32),
    record_md5 CHAR(32) PRIMARY KEY,
    latest_revision INT,
    FOREIGN KEY (wiki) REFERENCES wikis(id)
);

-- Add index on reference_md5 in wikireferences table for performance
CREATE INDEX idx_wikireferences_record_md5 ON wikireferences(record_md5);
