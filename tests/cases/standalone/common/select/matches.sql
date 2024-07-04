create table fox (
    ts timestamp time index,
    fox string,
);

insert into fox values
    (1, 'The quick brown fox jumps over the lazy dog'),
    (2, 'The             fox jumps over the lazy dog'),
    (3, 'The quick brown     jumps over the lazy dog'),
    (4, 'The quick brown fox       over the lazy dog'),
    (5, 'The quick brown fox jumps      the lazy dog'),
    (6, 'The quick brown fox jumps over          dog'),
    (7, 'The quick brown fox jumps over the      dog');

select fox from fox where matches(fox, '"fox jumps"') order by ts;

drop table fox;
