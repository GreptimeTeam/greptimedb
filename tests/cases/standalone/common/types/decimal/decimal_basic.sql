-- Basic tests for decimal type
-- TODO(QuenKar): port more decimal tests from DuckDB
create table decimals(
    ts timestamp time index,
    d decimal(10, 2),
);

insert into decimals values (1000, 1.23);

insert into decimals values
    (2000, 3.14),
    (3000, '123.45'),
    (4000, '1234.56');

select * from decimals;

-- math operations
select d + 1 from decimals;
select d - 1 from decimals;
select d * 2 from decimals;
select d / 2 from decimals;

-- aggregate functions
select sum(d) from decimals;
select avg(d) from decimals;
select max(d) from decimals;
select min(d) from decimals;
select count(d) from decimals;

drop table decimals;
