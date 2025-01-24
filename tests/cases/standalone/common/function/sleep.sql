select sleep(0.1);

select sleep(1) as a;

-- should fail it is for postgres
select pg_sleep(0.1);

-- SQLNESS PROTOCOL POSTGRES
select pg_sleep(0.5);

-- SQLNESS PROTOCOL POSTGRES
select pg_sleep(2) as b;
