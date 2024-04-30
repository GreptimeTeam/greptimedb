create table "HelloWorld" (a string, b timestamp time index);

insert into "HelloWorld" values ("a", 1) ,("b", 2);

select count(*) from "HelloWorld";

drop table "HelloWorld";

create table test (a string, "BbB" timestamp time index);

insert into test values ("c", 1) ;

select count(*) from test;

select count(*) from (select count(*) from test where a = 'a');

drop table test;
