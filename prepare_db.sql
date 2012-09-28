CREATE DATABASE kwesa_test;
USE kwesa_test;
CREATE TABLE page (page_id INTEGER NOT NULL PRIMARY KEY, text TEXT);

-- insert sample data
INSERT INTO page VALUES (1, "machine learning");
INSERT INTO page VALUES (2, "machine understanding");
INSERT INTO page VALUES (3, "data mining");
