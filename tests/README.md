# Sqlness Test

## Sqlness manual

### Case file
Sqlness has three types of file
- `.sql`: test input, SQL only
- `.result`: expected test output, SQL and its results
- `.output`: different output, SQL and its results

Both `.result` and `.output` are output (execution result) files. The difference is that `.result` is the
the standard (expected) output, and `.output` is the error output. Therefore, if you see `.output` files generated,
it means this test gets a different result and indicates it fails. You should
check change logs to solve the problem.  

You only need to write test SQL in `.sql` file, and run the test. On the first run it produces
an `.output` file because there is no `.result` to compare with. If you can make sure the content in
`.output` is correct, you can rename it to `.result`, which means it is the expected output.

And at any time there should only be two file types, `.sql` and `.result` -- otherwise, an existing `.output`
file means your test fails. That's why we should not ignore `.output` file type in `.gitignore`, instead, track
it and make sure it doesn't exist.

### Case organization
The root dir of input cases is `tests/cases`. It contains several sub-directories stand for different test
modes. E.g., `standalone/` contains all the tests to run under `greptimedb standalone start` mode.

Under the first level of sub-directory (e.g. the `cases/standalone`), you can organize your cases as you like.
Sqlness walks through every file recursively and runs them.

## Run the test
Unlike other tests, this harness is in a binary target form. You can run it with
```shell
cargo sqlness
```
It automatically finishes the following procedures: compile `GreptimeDB`, start it, grab tests and feed it to
the server, then collect and compare the results. You only need to check whether there are new `.output` files.
If not, congratulations, the test is passed 🥳!
