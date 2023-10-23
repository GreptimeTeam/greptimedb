create table demo(ts timestamp time index, `value` double, host string,idc string, collector string, primary key(host, idc, collector));

insert into demo values(1,2,'test1', 'idc1', 'disk') ,(2,3,'test2', 'idc1', 'disk'), (3,4,'test3', 'idc2','memory');

select * from demo where host='test1';

select * from demo where host='test2';

select * from demo where host='test3';

drop table demo;
