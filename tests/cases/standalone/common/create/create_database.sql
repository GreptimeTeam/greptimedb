create database illegal-database;

create database 'illegal-database';

create database '㊙️database';

show databases;

create database monitor;

-- error: database already exists
create database monitor;

-- nothing happens
create database if not exists monitor;

show databases;

use monitor;

create table cpus(
    id int,
    ts timestamp time index,
);

show tables;
