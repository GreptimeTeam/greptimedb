create table a(i integer, ts TIMESTAMP TIME INDEX);

insert into a values (42, 1);

with cte1 as (Select i as j from a) select * from cte1;

with cte1 as (with b as (Select i as j from a) Select j from b) select x from cte1 t1(x);

with cte1(xxx) as (with ncte(yyy) as (Select i as j from a) Select yyy from ncte) select xxx from cte1;

with cte1 as (with b as (Select i as j from a) select j from b), cte2 as (with c as (select ref.j+1 as k from cte1 as ref) select k from c) select * from cte1 , cte2;

with cte1 as (select 42), cte1 as (select 42) select * FROM cte1;

with cte1 as (Select i as j from a) select * from (with cte2 as (select max(j) as j from cte1) select * from cte2) f;

-- Refer to CTE in subquery expression,
-- this feature is not implemented in datafusion
with cte1 as (Select i as j from a) select * from cte1 where j = (with cte2 as (select max(j) as j from cte1) select j from cte2);

-- Refer to same-named CTE in a subquery expression
-- this feature is not implemented in datafusion
with cte as (Select i as j from a) select * from cte where j = (with cte as (select max(j) as j from cte) select j from cte);

-- self-refer to non-existent cte-
with cte as (select * from cte) select * from cte;

drop table a;
