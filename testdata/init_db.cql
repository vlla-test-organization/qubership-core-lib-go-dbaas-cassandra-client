CREATE KEYSPACE service_db WITH replication ={'class' : 'SimpleStrategy', 'replication_factor':1};

CREATE ROLE test_user WITH PASSWORD = 'test_password' AND LOGIN = true;
GRANT ALL PERMISSIONS on KEYSPACE service_db to test_user;

CREATE TABLE service_db.testObjects
(
    id   varchar,
    name varchar,
    PRIMARY KEY (id)
);

INSERT INTO service_db.testObjects(id, name)
values ('object1', 'test object 1');