create table http_requests (
    ts timestamp time index,
    job string,
    instance string,
    env string,
    greptime_value double,
    primary key (job, instance, env)
);

insert into http_requests values 
    (3000000, "api", "0", "production", 100),
    (3000000, "api", "0", "production", 200),
    (3000000, "api", "0", "canary", 300),
    (3000000, "api", "0", "canary", 400),
    (3000000, "app", "2", "production", 500),
    (3000000, "app", "2", "production", 600),
    (3000000, "app", "2", "canary", 700),
    (3000000, "app", "2", "canary", 800);

tql eval (3000, 3000, '1s') http_requests{job="api",instance="0",env="production"} + 5;

tql eval (3000, 3000, '1s') http_requests{job="web",instance="1",env="production"} * 5;

tql eval (3000, 3000, '1s') http_requests{job="web",instance="1",env="production"} * 5 or http_requests{job="api",instance="0",env="production"} + 5;

tql eval (3000, 3000, '1s') http_requests{job="api",instance="0",env="production"} + 5 or http_requests{job="web",instance="1",env="production"} * 5;

tql eval (3000, 3000, '1s') http_requests{job="web",instance="1",env="production"} * 5 or http_requests{job="web",instance="2",env="production"} * 3 or http_requests{job="api",instance="0",env="production"} + 5;

drop table http_requests;
