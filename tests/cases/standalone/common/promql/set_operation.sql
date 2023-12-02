-- from promql/testdata/operators.test
-- cases related to AND/OR/UNLESS
-- group_left() and group_right() are not included

create table http_requests (
    ts timestamp time index,
    job string,
    instance string,
    g string, -- for `group`
    val double,
    primary key (job, instance, g)
);

insert into http_requests values 
    (3000000, "api", "0", "production", 100),
    (3000000, "api", "1", "production", 200),
    (3000000, "api", "0", "canary", 300),
    (3000000, "api", "1", "canary", 400),
    (3000000, "app", "0", "production", 500),
    (3000000, "app", "1", "production", 600),
    (3000000, "app", "0", "canary", 700),
    (3000000, "app", "1", "canary", 800);

-- empty metric
create table cpu_count(ts timestamp time index);

create table vector_matching_a(
    ts timestamp time index,
    l string primary key,
    val double,
);

insert into vector_matching_a values
    (3000000, "x", 10),
    (3000000, "y", 20);

-- eval instant at 50m http_requests{group="canary"} and http_requests{instance="0"}
-- 	http_requests{group="canary", instance="0", job="api-server"} 300
-- 	http_requests{group="canary", instance="0", job="app-server"} 700
-- SQLNESS SORT_RESULT 3 1
tql eval (3000, 3000, '1s') http_requests{g="canary"} and http_requests{instance="0"};

-- eval instant at 50m (http_requests{group="canary"} + 1) and http_requests{instance="0"}
-- 	{group="canary", instance="0", job="api-server"} 301
-- 	{group="canary", instance="0", job="app-server"} 701
-- SQLNESS SORT_RESULT 3 1
tql eval (3000, 3000, '1s') (http_requests{g="canary"} + 1) and http_requests{instance="0"};

-- eval instant at 50m (http_requests{group="canary"} + 1) and on(instance, job) http_requests{instance="0", group="production"}
-- 	{group="canary", instance="0", job="api-server"} 301
-- 	{group="canary", instance="0", job="app-server"} 701
-- SQLNESS SORT_RESULT 3 1
tql eval (3000, 3000, '1s') (http_requests{g="canary"} + 1) and on(instance, job) http_requests{instance="0", g="production"};

-- eval instant at 50m (http_requests{group="canary"} + 1) and on(instance) http_requests{instance="0", group="production"}
-- 	{group="canary", instance="0", job="api-server"} 301
-- 	{group="canary", instance="0", job="app-server"} 701
-- SQLNESS SORT_RESULT 3 1
tql eval (3000, 3000, '1s') (http_requests{g="canary"} + 1) and on(instance) http_requests{instance="0", g="production"};

-- eval instant at 50m (http_requests{group="canary"} + 1) and ignoring(group) http_requests{instance="0", group="production"}
-- 	{group="canary", instance="0", job="api-server"} 301
-- 	{group="canary", instance="0", job="app-server"} 701
-- SQLNESS SORT_RESULT 3 1
tql eval (3000, 3000, '1s') (http_requests{g="canary"} + 1) and ignoring(g) http_requests{instance="0", g="production"};

-- eval instant at 50m (http_requests{group="canary"} + 1) and ignoring(group, job) http_requests{instance="0", group="production"}
-- 	{group="canary", instance="0", job="api-server"} 301
-- 	{group="canary", instance="0", job="app-server"} 701
-- SQLNESS SORT_RESULT 3 1
tql eval (3000, 3000, '1s') (http_requests{g="canary"} + 1) and ignoring(g, job) http_requests{instance="0", g="production"};

-- eval instant at 50m http_requests{group="canary"} or http_requests{group="production"}
-- 	http_requests{group="canary", instance="0", job="api-server"} 300
-- 	http_requests{group="canary", instance="0", job="app-server"} 700
-- 	http_requests{group="canary", instance="1", job="api-server"} 400
-- 	http_requests{group="canary", instance="1", job="app-server"} 800
-- 	http_requests{group="production", instance="0", job="api-server"} 100
-- 	http_requests{group="production", instance="0", job="app-server"} 500
-- 	http_requests{group="production", instance="1", job="api-server"} 200
-- 	http_requests{group="production", instance="1", job="app-server"} 600
-- NOT SUPPORTED: `or`
tql eval (3000, 3000, '1s') http_requests{g="canary"} or http_requests{g="production"};

-- # On overlap the rhs samples must be dropped.
-- eval instant at 50m (http_requests{group="canary"} + 1) or http_requests{instance="1"}
-- 	{group="canary", instance="0", job="api-server"} 301
-- 	{group="canary", instance="0", job="app-server"} 701
-- 	{group="canary", instance="1", job="api-server"} 401
-- 	{group="canary", instance="1", job="app-server"} 801
-- 	http_requests{group="production", instance="1", job="api-server"} 200
-- 	http_requests{group="production", instance="1", job="app-server"} 600
-- NOT SUPPORTED: `or`
tql eval (3000, 3000, '1s') (http_requests{g="canary"} + 1) or http_requests{instance="1"};


-- # Matching only on instance excludes everything that has instance=0/1 but includes
-- # entries without the instance label.
-- eval instant at 50m (http_requests{group="canary"} + 1) or on(instance) (http_requests or cpu_count or vector_matching_a)
-- 	{group="canary", instance="0", job="api-server"} 301
-- 	{group="canary", instance="0", job="app-server"} 701
-- 	{group="canary", instance="1", job="api-server"} 401
-- 	{group="canary", instance="1", job="app-server"} 801
-- 	vector_matching_a{l="x"} 10
-- 	vector_matching_a{l="y"} 20
-- NOT SUPPORTED: union on different schemas
-- NOT SUPPORTED: `or`
tql eval (3000, 3000, '1s') (http_requests{g="canary"} + 1) or on(instance) (http_requests or cpu_count or vector_matching_a);

-- eval instant at 50m (http_requests{group="canary"} + 1) or ignoring(l, group, job) (http_requests or cpu_count or vector_matching_a)
-- 	{group="canary", instance="0", job="api-server"} 301
-- 	{group="canary", instance="0", job="app-server"} 701
-- 	{group="canary", instance="1", job="api-server"} 401
-- 	{group="canary", instance="1", job="app-server"} 801
-- 	vector_matching_a{l="x"} 10
-- 	vector_matching_a{l="y"} 20
-- NOT SUPPORTED: union on different schemas
-- NOT SUPPORTED: `or`
tql eval (3000, 3000, '1s') (http_requests{g="canary"} + 1) or ignoring(l, g, job) (http_requests or cpu_count or vector_matching_a);

-- eval instant at 50m http_requests{group="canary"} unless http_requests{instance="0"}
-- 	http_requests{group="canary", instance="1", job="api-server"} 400
-- 	http_requests{group="canary", instance="1", job="app-server"} 800
-- SQLNESS SORT_RESULT 3 1
tql eval (3000, 3000, '1s') http_requests{g="canary"} unless http_requests{instance="0"};

-- eval instant at 50m http_requests{group="canary"} unless on(job) http_requests{instance="0"}
tql eval (3000, 3000, '1s') http_requests{g="canary"} unless on(job) http_requests{instance="0"};

-- eval instant at 50m http_requests{group="canary"} unless on(job, instance) http_requests{instance="0"}
-- 	http_requests{group="canary", instance="1", job="api-server"} 400
-- 	http_requests{group="canary", instance="1", job="app-server"} 800
-- SQLNESS SORT_RESULT 3 1
tql eval (3000, 3000, '1s') http_requests{g="canary"} unless on(job, instance) http_requests{instance="0"};

-- eval instant at 50m http_requests{group="canary"} unless ignoring(group, instance) http_requests{instance="0"}
tql eval (3000, 3000, '1s') http_requests{g="canary"} unless ignoring(g, instance) http_requests{instance="0"};

-- eval instant at 50m http_requests{group="canary"} unless ignoring(group) http_requests{instance="0"}
-- 	http_requests{group="canary", instance="1", job="api-server"} 400
-- 	http_requests{group="canary", instance="1", job="app-server"} 800
-- SQLNESS SORT_RESULT 3 1
tql eval (3000, 3000, '1s') http_requests{g="canary"} unless ignoring(g) http_requests{instance="0"};


-- # https://github.com/prometheus/prometheus/issues/1489
-- eval instant at 50m http_requests AND ON (dummy) vector(1)
-- 	http_requests{group="canary", instance="0", job="api-server"} 300
-- 	http_requests{group="canary", instance="0", job="app-server"} 700
-- 	http_requests{group="canary", instance="1", job="api-server"} 400
-- 	http_requests{group="canary", instance="1", job="app-server"} 800
-- 	http_requests{group="production", instance="0", job="api-server"} 100
-- 	http_requests{group="production", instance="0", job="app-server"} 500
-- 	http_requests{group="production", instance="1", job="api-server"} 200
-- 	http_requests{group="production", instance="1", job="app-server"} 600
-- NOT SUPPORTED: `vector()`
tql eval (3000, 3000, '1s') http_requests AND ON (dummy) vector(1);

-- eval instant at 50m http_requests AND IGNORING (group, instance, job) vector(1)
-- 	http_requests{group="canary", instance="0", job="api-server"} 300
-- 	http_requests{group="canary", instance="0", job="app-server"} 700
-- 	http_requests{group="canary", instance="1", job="api-server"} 400
-- 	http_requests{group="canary", instance="1", job="app-server"} 800
-- 	http_requests{group="production", instance="0", job="api-server"} 100
-- 	http_requests{group="production", instance="0", job="app-server"} 500
-- 	http_requests{group="production", instance="1", job="api-server"} 200
-- 	http_requests{group="production", instance="1", job="app-server"} 600
-- NOT SUPPORTED: `vector()`
tql eval (3000, 3000, '1s') http_requests AND IGNORING (g, instance, job) vector(1);

drop table http_requests;

drop table cpu_count;

drop table vector_matching_a;
