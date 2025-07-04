---
Feature Name: Compatibility Test Framework
Tracking Issue: TBD
Date: 2025-07-04
Author: "Ruihang Xia <waynestxia@gmail.com>"
---

# Summary

This RFC proposes a compatibility test framework for GreptimeDB to ensure backward/forward compatibility for different versions of GreptimeDB.

# Motivation

In current practice, we don't have a systematic way to test and ensure the compatibility of different versions of GreptimeDB. Each time we release a new version, we need to manually test the compatibility with ad-hoc cases. This is not only time-consuming, but also prone to errors and unmaintainable. Highly rely on the release manager to ensure the compatibility of different versions of GreptimeDB.

We don't have a detailed guide on the release SoP of how to test and ensure the compatibility of the new version. And has broken the compatibility of the new version many times (`v0.14.1` and `v0.15.1` are two examples, which are both released right after the major release).

# Details

This RFC proposes a compatibility test framework that is easy to maintain, extend and run. It can tell the compatibility between any given two versions of GreptimeDB, both backward and forward. It's based on the Sqlness library but used in a different way.

Generally speaking, the framework is composed of two parts:

1. Test cases: A set of test cases that are maintained dedicatedly for the compatibility test. Still in the `.sql` and `.result` format.
2. Test framework: A new sqlness runner that is used to run the test cases. With some new features that is not required by the integration sqlness test.

## Test Cases

### Structure

The case set is organized in three parts:

- `1.feature`: Use a new feature
- `2.verify`: Verify database behavior
- `3.cleanup`: Paired with `1.feature`, cleanup the test environment.

These three parts are organized in a tree structure, and should be run in sequence:

```
compatibility_test/
├── 1.feature/
│   ├── feature-a/
│   ├── feature-b/
│   └── feature-c/
├── 2.verify/
│   ├── verify-metadata/
│   ├── verify-data/
│   └── verify-schema/
└── 3.cleanup/
    ├── cleanup-a/
    ├── cleanup-b/
    └── cleanup-c/
```

### Example

For example, for a new feature like adding new index option ([#6416](https://github.com/GreptimeTeam/greptimedb/pull/6416)), we (who implement the feature) create a new test case like this:

```sql
-- path: compatibility_test/1.feature/index-option/granularity_and_false_positive_rate.sql

-- SQLNESS ARG since=0.15.0
-- SQLNESS IGNORE_RESULT
CREATE TABLE granularity_and_false_positive_rate (ts timestamp time index, val double) with ("index.granularity" = "8192", "index.false_positive_rate" = "0.01");
```

And

```sql
-- path: compatibility_test/3.cleanup/index-option/granularity_and_false_positive_rate.sql
drop table granularity_and_false_positive_rate;
```

Since this new feature don't require some special way to verify the database behavior, we can reuse existing test cases in `2.verify/` to verify the database behavior. For example, we can reuse the `verify-metadata` test case to verify the metadata of the table.

```sql
-- path: compatibility_test/2.verify/verify-metadata/show-create-table.sql

-- SQLNESS TEMPLATE TABLE="SHOW TABLES";
SHOW CREATE TABLE $TABLE;
```

In this example, we use some new sqlness features that will be introduced in the next section (`since`, `IGNORE_RESULT`, `TEMPLATE`).

### Maintaince

Each time implement a new feature that should be covered by the compatibility test, we should create a new test case in `1.feature/` and `3.cleanup/` for them. And check if existing cases in `2.verify/` can be reused to verify the database behavior.

This simulates an enthusiastic user who uses all the new features at the first time. All the new maintaince burden is on the feature implementer to write one more test case for the new feature, to "fixation" the behavior. And once there is a breaking change in the future, it can be detected by the compatibility test framework automatically.

Another topic is about deprecation. If a feature is deprecated, we should also mark it in the test case. Still use above example, assume we deprecate the `index.granularity` and `index.false_positive_rate` index options in `v0.99.0`, we can mark them as:
```sql
-- SQLNESS ARG since=0.15.0 till=0.99.0
...
```

This tells the framework to ignore this feature in version `v0.99.0` and later. Currently, we have so many experimental features that are scheduled to be broken in the future, this is a good way to mark them.

## Test Framework

This section is about new sqlness features required by this framework.

### Since and Till

Follows the `ARG` interceptor in sqlness, we can mark a feature is available between two given versions. Only the `since` is required:

```sql
-- SQLNESS ARG since=VERSION_STRING [till=VERSION_STRING]
```

### IGNORE_RESULT

`IGNORE_RESULT` is a new interceptor, it tells the runner to ignore the result of the query, only check whether the query is executed successfully.

This is useful to reduce the maintaince burden of the test cases, unlike the integration sqlness test, in most cases we don't care about the result of the query, only need to make sure the query is executed successfully.

### TEMPLATE

`TEMPLATE` is another new interceptor, it can generate queries from a template based on a runtime data.

In above exmample, we need to run the `SHOW CREATE TABLE` query for all existing tables, so we can use the `TEMPLATE` interceptor to generate the query with a dynamic table list.

### RUNNER

There are also some extra requirement for the runner itself:

- It should run the test cases in sequence, first `1.feature/`, then `2.verify/`, and finally `3.cleanup/`.
- It should be able to fetch required version automatically to finish the test.
- It should handle the `since` and `till` properly.

On the `1.feature` phase, the runner needs to identify all features need to be tested by version number. And then restart with a new version (the `to` version) to run `2.verify/` and `3.cleanup/` phase.

## Test Report

Finally, we can run the compatibility test to verify the compatibility between any given two versions of GreptimeDB, for example:

```bash
# check backward compatibility between v0.15.0 and v0.16.0 when releasing v0.16.0
./sqlness run --from=0.15.0 --to=0.16.0

# check forward compatibility when downgrading from v0.15.0 to v0.13.0
./sqlness run --from=0.15.0 --to=0.13.0
```

We can also use a script to run the compatibility test for all the versions in a given range to give a quick report with all versions we need.

And we always bump the version in `Cargo.toml` to the next major release version, so the next major release version can be used as "latest" unpublished version for scenarios like local testing.

# Alternatives

There was a previous attempt to implement a compatibility test framework that was disabled due to some reasons [#3728](https://github.com/GreptimeTeam/greptimedb/issues/3728).
