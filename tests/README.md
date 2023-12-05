# Sqlness Test

## Sqlness manual

### Case file

Sqlness has two types of file:

- `.sql`: test input, SQL only
- `.result`: expected test output, SQL and its results

`.result` is the output (execution result) file. If you see `.result` files is changed,
it means this test gets a different result and indicates it fails. You should
check change logs to solve the problem.  

You only need to write test SQL in `.sql` file, and run the test.

### Case organization

The root dir of input cases is `tests/cases`. It contains several subdirectories stand for different test
modes. E.g., `standalone/` contains all the tests to run under `greptimedb standalone start` mode.

Under the first level of subdirectory (e.g. the `cases/standalone`), you can organize your cases as you like.
Sqlness walks through every file recursively and runs them.

## Run the test

Unlike other tests, this harness is in a binary target form. You can run it with:

```shell
cargo sqlness
```

It automatically finishes the following procedures: compile `GreptimeDB`, start it, grab tests and feed it to
the server, then collect and compare the results. You only need to check if the `.result` files are changed.
If not, congratulations, the test is passed 🥳!
