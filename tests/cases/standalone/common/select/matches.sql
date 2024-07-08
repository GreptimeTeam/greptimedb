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

select fox from fox where matches(fox, '"quick brown"') order by ts;

select fox from fox where matches(fox, '"fox jumps"') order by ts;

select fox from fox where matches(fox, 'fox OR lazy') order by ts;

select fox from fox where matches(fox, 'fox AND lazy') order by ts;

select fox from fox where matches(fox, '-over -lazy') order by ts;

select fox from fox where matches(fox, '-over AND -lazy') order by ts;

select fox from fox where matches(fox, 'fox AND jumps OR over') order by ts;

select fox from fox where matches(fox, 'fox OR brown AND quick') order by ts;

select fox from fox where matches(fox, '(fox OR brown) AND quick') order by ts;

select fox from fox where matches(fox, 'brown AND quick OR fox') order by ts;

select fox from fox where matches(fox, 'brown AND (quick OR fox)') order by ts;

select fox from fox where matches(fox, 'brown AND quick AND fox  OR  jumps AND over AND lazy') order by ts;

select fox from fox where matches(fox, 'quick brown fox +jumps') order by ts;

select fox from fox where matches(fox, 'fox +jumps -over') order by ts;

select fox from fox where matches(fox, 'fox AND +jumps AND -over') order by ts;

select fox from fox where matches(fox, '(+fox +jumps) over') order by ts;

select fox from fox where matches(fox, '+(fox jumps) AND over') order by ts;

select fox from fox where matches(fox, 'over -(fox jumps)') order by ts;

select fox from fox where matches(fox, 'over -(fox AND jumps)') order by ts;

select fox from fox where matches(fox, 'over AND -(-(fox OR jumps))') order by ts;

drop table fox;
