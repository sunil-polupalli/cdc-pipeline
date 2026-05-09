-- 03-users.sql
-- Create Debezium replication user with required privileges

USE mysql;

CREATE USER IF NOT EXISTS 'debezium_user'@'%' IDENTIFIED WITH mysql_native_password BY 'debezium_pw';

GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium_user'@'%';

FLUSH PRIVILEGES;
