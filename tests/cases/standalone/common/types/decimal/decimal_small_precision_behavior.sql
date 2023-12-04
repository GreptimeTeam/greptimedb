select '1.023450000001'::DECIMAL(5,4);

select '1.234499999'::DECIMAL(4,3);

select '1.23499999'::DECIMAL(4,3);

select '1.234499999'::DECIMAL(5,4);

-- arrow-rs is a little strange about the conversion behavior of negative numbers.
-- issue: https://github.com/apache/arrow-datafusion/issues/8326
select '-1.023450000001'::DECIMAL(5,4);

select '-1.234499999'::DECIMAL(4,3);

select '-1.23499999'::DECIMAL(4,3);

select '-1.234499999'::DECIMAL(5,4);
