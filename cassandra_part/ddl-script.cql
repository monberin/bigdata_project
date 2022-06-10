CREATE KEYSPACE project WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1 };
USE project;
DROP TABLE IF EXISTS domains;
CREATE TABLE IF NOT EXISTS domains (
    domain_ text,
    uri text,
    PRIMARY KEY (domain_, uri));

DROP TABLE IF EXISTS user_pages;
CREATE TABLE IF NOT EXISTS user_pages (
    user_id text,
    uri text,
    PRIMARY KEY (user_id, uri));

DROP TABLE IF EXISTS page_ids;
CREATE TABLE IF NOT EXISTS page_ids (
    page_id text,
    uri text,
    PRIMARY KEY (page_id, uri));

DROP TABLE IF EXISTS users;
CREATE TABLE IF NOT EXISTS users (
    user_id text,
    user_text text,
    uri text,
    created_at timestamp,
    PRIMARY KEY (user_id, created_at));

-- docker exec -it node1 cqlsh -e