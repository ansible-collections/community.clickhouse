CREATE DATABASE IF NOT EXISTS test_db;
CREATE TABLE IF NOT EXISTS test_db.test_table
(
    date Date,
    userID String,
    action LowCardinality(String),
    counter UInt64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date)
PRIMARY KEY (date, action)
ORDER BY (date, action, userID);
INSERT INTO test_db.test_table VALUES({d:Date}, 'ab', {a:String}, 1);
INSERT INTO test_db.test_table VALUES({d:Date}, 'ac', {a:String}, 2);