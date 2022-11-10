# Contributing to GreptimeDB

Much appreciate for your interest in contributing to GreptimeDB! This document list some guidelines for contributing to our code base.

To learn about the design of GreptimeDB, please refer to the [design docs](https://github.com/GrepTimeTeam/docs).

## Pull Requests

### Before PR
- Make sure all unit tests are passed.
- Make sure all clippy warnings are fixed (you can check it locally by running `cargo clippy --workspace --all-targets -- -D warnings -D clippy::print_stdout -D clippy::print_stderr`).

#### `pre-commit` Hooks
You could setup the [`pre-commit`](https://pre-commit.com/#plugins) hooks to run these checks on every commit automatically.

1. Install `pre-commit`
```
$ pip install pre-commit
```
or
```
$ brew install pre-commit
```

2. Install the `pre-commit` hooks
```
$ pre-commit install
pre-commit installed at .git/hooks/pre-commit

$ pre-commit install --hook-type commit-msg
pre-commit installed at .git/hooks/commit-msg

$ pre-commit install --hook-type pre-push
pre-commit installed at .git/hooks/pre-pus
```

now `pre-commit` will run automatically on `git commit`.

### Title

The titles of pull requests should be prefixed with one of the change types listed in [Conventional Commits specification](https://www.conventionalcommits.org/en/v1.0.0) 
like `feat`/`fix`/`docs`, with a concise summary of code change follows. The following scope field is optional, you can fill it with the name of sub-crate if the pull request only changes one, or just leave it blank.

### Description

- If your pull request is small, like a typo fix, feel free to go brief.
- But if it contains large code change, make sure to state the motivation/design details of this PR so that reviewers can understand what you're trying to do.
- If the PR contains any breaking change or API change, make sure that is clearly listed in your description.

## Getting help

There are many ways to get help when you're stuck. It is recommended to ask for help by opening an issue, with a detailed description
of what you were trying to do and what went wrong. You can also reach for help in our Slack channel.

## Bug report
To report a bug or a security issue, you can [open a new GitHub issue](https://github.com/GrepTimeTeam/greptimedb/issues/new).
