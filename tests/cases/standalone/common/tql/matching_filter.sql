create table gauge_metric(
    ts timestamp time index,
    host string,
    device string,
    val double,
    primary key (host, device)
);

insert into gauge_metric values
    (0, 'host1', 'eth0', 2),
    (0, 'host1', 'eth1', 2),
    (0, 'host2', 'eth0', 2),
    (5000, 'host1', 'eth0', 2),
    (5000, 'host1', 'eth1', 2),
    (5000, 'host2', 'eth0', 2);

create table counter_metric(
    ts timestamp time index,
    host string,
    device string,
    val double,
    primary key (host, device)
);

insert into counter_metric values
    (0, 'host1', 'eth0', 10),
    (0, 'host1', 'eth1', 20),
    (0, 'host2', 'eth0', 40),
    (5000, 'host1', 'eth0', 10),
    (5000, 'host1', 'eth1', 20),
    (5000, 'host2', 'eth0', 40);

-- Default matching.
-- SQLNESS SORT_RESULT 3 1
tql eval(0, 5, '5s') counter_metric / gauge_metric;

-- SQLNESS SORT_RESULT 3 1
tql eval(0, 5, '5s') counter_metric / gauge_metric{host="host1"};

-- SQLNESS SORT_RESULT 3 1
tql eval(0, 5, '5s') counter_metric / {__name__="gauge_metric", host="host1"};

-- Explicit `on(...)`.
-- SQLNESS SORT_RESULT 3 1
tql eval(0, 5, '5s') counter_metric / on(host, device) gauge_metric{host="host1"};

-- SQLNESS SORT_RESULT 3 1
tql eval(0, 5, '5s') counter_metric{host="host1"} / on(host, device) gauge_metric{device="eth0"};

-- Conflicting constraints on the same label.
-- SQLNESS SORT_RESULT 3 1
tql eval(0, 5, '5s') counter_metric{host="host1"} / on(host, device) gauge_metric{host="host2"};

-- `val` is a value field of both metrics, not a label, even when named in `on(...)`.
-- SQLNESS SORT_RESULT 3 1
tql eval(0, 5, '5s') counter_metric / gauge_metric{val="2"};

-- SQLNESS SORT_RESULT 3 1
tql eval(0, 5, '5s') counter_metric / on(host, device, val) gauge_metric{val="2"};

-- A matcher on a name that is not a column of the metric.
-- SQLNESS SORT_RESULT 3 1
tql eval(0, 5, '5s') counter_metric / gauge_metric{missing_label="x"};

-- An empty matcher value also matches an absent label.
-- SQLNESS SORT_RESULT 3 1
tql eval(0, 5, '5s') counter_metric / on(host, device) gauge_metric{device=""};

-- Range functions over a matrix selector, and offsets.
-- SQLNESS SORT_RESULT 3 1
tql eval(0, 5, '5s') count_over_time(counter_metric[10s]) / on(host, device) count_over_time(gauge_metric{host="host1"}[10s]);

-- SQLNESS SORT_RESULT 3 1
tql eval(0, 5, '5s') counter_metric offset 5s / on(host, device) gauge_metric{host="host1"};

-- Matching on a subset of the labels. Prometheus rejects this as many-to-many; GreptimeDB
-- returns the cross product instead (#9209). Recorded here to show the rewrite does not
-- change it, not to endorse it.
-- SQLNESS SORT_RESULT 3 1
tql eval(0, 5, '5s') counter_metric / on(host) gauge_metric{host="host1"};

-- `ignoring(...)`: every label that is not ignored is a matching label.
-- SQLNESS SORT_RESULT 3 1
tql eval(0, 5, '5s') counter_metric / ignoring(missing_label) gauge_metric{host="host1"};

-- SQLNESS SORT_RESULT 3 1
tql eval(0, 5, '5s') counter_metric / ignoring(device) gauge_metric{host="host1"};

-- An ignored label is not a matching label: the left operand keeps its `eth1` series.
-- SQLNESS SORT_RESULT 3 1
tql eval(0, 5, '5s') counter_metric / ignoring(device) gauge_metric{device="eth0"};

-- Same for a label left out of `on(...)`.
-- SQLNESS SORT_RESULT 3 1
tql eval(0, 5, '5s') counter_metric / on(host) gauge_metric{device="eth0"};

-- Matcher kinds other than equality are enforced by the join just the same.
-- SQLNESS SORT_RESULT 3 1
tql eval(0, 5, '5s') counter_metric / on(host, device) gauge_metric{host=~"host1"};

-- SQLNESS SORT_RESULT 3 1
tql eval(0, 5, '5s') counter_metric / on(host, device) gauge_metric{host!="host2"};

-- SQLNESS SORT_RESULT 3 1
tql eval(0, 5, '5s') counter_metric / on(host, device) gauge_metric{device!~"eth1"};

-- Aggregations that partition by their grouping labels.
-- SQLNESS SORT_RESULT 3 1
tql eval(0, 5, '5s') sum by(host)(counter_metric) / on(host) sum by(host)(gauge_metric{host="host1"});

-- SQLNESS SORT_RESULT 3 1
tql eval(0, 5, '5s') sum by(host)(counter_metric{device="eth0"}) / on(host) sum by(host)(gauge_metric);

-- A label the aggregation drops is not a matching label.
-- SQLNESS SORT_RESULT 3 1
tql eval(0, 5, '5s') counter_metric / on(host) sum by(host)(gauge_metric{device="eth0"});

-- `topk` preserves its input labels, and filtering before `topk` changes the set of
-- candidates it ranks, so the rewrite must not reach its input.
-- SQLNESS SORT_RESULT 3 1
tql eval(0, 5, '5s') topk(1, counter_metric) / on(host, device) gauge_metric{host="host1"};

-- Operators and modifiers outside the rewritten subset.
-- SQLNESS SORT_RESULT 3 1
tql eval(0, 5, '5s') counter_metric > on(host, device) gauge_metric{host="host1"};

-- SQLNESS SORT_RESULT 3 1
tql eval(0, 5, '5s') counter_metric / on(host) group_left sum by(host)(gauge_metric{host="host1"});

drop table gauge_metric;

drop table counter_metric;
