# Welcome 👋

Thanks a lot for considering contributing to GreptimeDB. We believe people like you would make GreptimeDB a great product. We intend to build a community where individuals can have open talks, show respect for one another, and speak with true ❤️. Meanwhile, we are to keep transparency and make your effort count here.

You can find our contributors at https://github.com/GreptimeTeam/greptimedb/graphs/contributors. When you dedicate to GreptimeDB for a few months and keep bringing high-quality contributions (code, docs, advocate, etc.), you will be a candidate of a committer.

A committer will be granted both read & write access to GreptimeDB repos. Check the [AUTHOR.md](AUTHOR.md) file for all current individual committers.

Please read the guidelines, and they can help you get started. Communicate respectfully with the developers maintaining and developing the project. In return, they should reciprocate that respect by addressing your issue, reviewing changes, as well as helping finalize and merge your pull requests.

Follow our [README](https://github.com/GreptimeTeam/greptimedb#readme) to get the whole picture of the project. To learn about the design of GreptimeDB, please refer to the [design docs](https://github.com/GrepTimeTeam/docs).

## Your First Contribution

It can feel intimidating to contribute to a complex project, but it can also be exciting and fun. These general notes will help everyone participate in this communal activity.

- Follow the [Code of Conduct](https://github.com/GreptimeTeam/.github/blob/main/.github/CODE_OF_CONDUCT.md)
- Small changes make huge differences. We will happily accept a PR making a single character change if it helps move forward. Don't wait to have everything working.
- Check the closed issues before opening your issue.
- Try to follow the existing style of the code.
- More importantly, when in doubt, ask away.

Pull requests are great, but we accept all kinds of other help if you like. Such as

- Write tutorials or blog posts. Blog, speak about, or create tutorials about one of GreptimeDB's many features. Mention [@greptime](https://twitter.com/greptime) on Twitter and email info@greptime.com so we can give pointers and tips and help you spread the word by promoting your content on Greptime communication channels.
- Improve the documentation. [Submit documentation](http://github.com/greptimeTeam/docs/) updates, enhancements, designs, or bug fixes, and fixing any spelling or grammar errors will be very much appreciated.
- Present at meetups and conferences about your GreptimeDB projects. Your unique challenges and successes in building things with GreptimeDB can provide great speaking material. We'd love to review your talk abstract, so get in touch with us if you'd like some help!
- Submitting bug reports. To report a bug or a security issue, you can [open a new GitHub issue](https://github.com/GrepTimeTeam/greptimedb/issues/new).
- Speak up feature requests. Send feedback is a great way for us to understand your different use cases of GreptimeDB better. If you want to share your experience with GreptimeDB, or if you want to discuss any ideas, you can start a discussion on [GitHub discussions](https://github.com/GreptimeTeam/greptimedb/discussions), chat with the Greptime team on [Slack](https://greptime.com/slack), or you can tweet [@greptime](https://twitter.com/greptime) on Twitter.

## Code of Conduct

Also, there are things that we are not looking for because they don't match the goals of the product or benefit the community. Please read [Code of Conduct](https://github.com/GreptimeTeam/.github/blob/main/.github/CODE_OF_CONDUCT.md); we hope everyone can keep good manners and become an honored member.

## License

GreptimeDB uses the [Apache 2.0 license](https://github.com/GreptimeTeam/greptimedb/blob/master/LICENSE) to strike a balance between open contributions and allowing you to use the software however you want.

## Getting Started

### Submitting Issues

- Check if an issue already exists. Before filing an issue report, see whether it's already covered. Use the search bar and check out existing issues.
- File an issue:
  - To report a bug, a security issue, or anything that you think is a problem and that isn't under the radar, go ahead and [open a new GitHub issue](https://github.com/GrepTimeTeam/greptimedb/issues/new).
  - In the given templates, look for the one that suits you.
  - If you bump into anything, reach out to our [Slack](https://greptime.com/slack) for a wider audience and ask for help.
- What happens after:
  - Once we spot a new issue, we identify and categorize it as soon as possible.
  - Usually, it gets assigned to other developers. Follow up and see what folks are talking about and how they take care of it.
  - Please be patient and offer as much information as you can to help reach a solution or a consensus. You are not alone and embrace team power.

### Before PR

- To ensure that community is free and confident in its ability to use your contributions, please sign the Contributor License Agreement (CLA) which will be incorporated in the pull request process.
- Make sure all files have proper license header (running `docker run --rm -v $(pwd):/github/workspace ghcr.io/korandoru/hawkeye-native:v3 format` from the project root).
- Make sure all your codes are formatted and follow the [coding style](https://pingcap.github.io/style-guide/rust/) and [style guide](docs/style-guide.md).
- Make sure all unit tests are passed using [nextest](https://nexte.st/index.html) `cargo nextest run --workspace --features pg_kvbackend,mysql_kvbackend` or `make test`.
- Make sure all clippy warnings are fixed (you can check it locally by running `cargo clippy --workspace --all-targets -- -D warnings` or `make clippy`).
- Ensure there are no unused dependencies by running `make check-udeps` (clean them up with `make fix-udeps` if reported).
- If you must keep a target-specific dependency (e.g. under `[target.'cfg(...)'.dev-dependencies]`), add a cargo-udeps ignore entry in the same `Cargo.toml`, for example:
  `[package.metadata.cargo-udeps.ignore]` with `development = ["rexpect"]` (or `dependencies`/`build` as appropriate).
- When modifying sample configuration files in `config/`, run `make config-docs` (which requires Docker to be installed) to update the configuration documentation and include it in your commit.

#### `pre-commit` Hooks

You could setup the [`pre-commit`](https://pre-commit.com/#plugins) hooks to run these checks on every commit automatically.

1.  Install `pre-commit`

        pip install pre-commit

    or

        brew install pre-commit

2.  Install the `pre-commit` hooks

        $ pre-commit install
        pre-commit installed at .git/hooks/pre-commit

        $ pre-commit install --hook-type commit-msg
        pre-commit installed at .git/hooks/commit-msg

        $ pre-commit install --hook-type pre-push
        pre-commit installed at .git/hooks/pre-push

Now, `pre-commit` will run automatically on `git commit`.

### Title

The titles of pull requests should be prefixed with category names listed in [Conventional Commits specification](https://www.conventionalcommits.org/en/v1.0.0)
like `feat`/`fix`/`docs`, with a concise summary of code change following. AVOID using the last commit message as pull request title.

### Description

- Feel free to go brief if your pull request is small, like a typo fix.
- But if it contains large code change, make sure to state the motivation/design details of this PR so that reviewers can understand what you're trying to do.
- If the PR contains any breaking change or API change, make sure that is clearly listed in your description.

### Commit Messages

All commit messages SHOULD adhere to the [Conventional Commits specification](https://conventionalcommits.org/).

## AI-Assisted contributions

We have the following policy for AI-assisted PRs:

- The PR author should **understand the core ideas** behind the implementation **end-to-end**, and be able to justify the design and code during review.
- **Calls out unknowns and assumptions**. It's okay to not fully understand some bits of AI generated code. You should comment on these cases and point them out to reviewers so that they can use their knowledge of the codebase to clear up any concerns. For example, you might comment "calling this function here seems to work but I'm not familiar with how it works internally, I wonder if there's a race condition if it is called concurrently".

### Why fully AI-generated PRs without understanding are not helpful

Today, AI tools cannot reliably make complex changes to GreptimeDB on their own, which is why we rely on pull requests and code review.

The purposes of code review are:

1. Finish the intended task.
2. Share knowledge between authors and reviewers, as a long-term investment in the project. For this reason, even if someone familiar with the codebase can finish a task quickly, we're still happy to help a new contributor work on it even if it takes longer.

An AI dump for an issue doesn’t meet these purposes. Maintainers could finish the task faster by using AI directly, and the submitters gain little knowledge if they act only as a pass through AI proxy without understanding.

Please understand the reviewing capacity is **very limited** for the project, so large PRs which appear to not have the requisite understanding might not get reviewed, and eventually closed or redirected.

### Better ways to contribute than an “AI dump”

It's recommended to write a high-quality issue with a clear problem statement and a minimal, reproducible example. This can make it easier for others to contribute.

## Getting Help

There are many ways to get help when you're stuck. It is recommended to ask for help by opening an issue, with a detailed description
of what you were trying to do and what went wrong. You can also reach for help in our [Slack channel](https://greptime.com/slack).

## Community

The core team will be thrilled if you would like to participate in any way you like. When you are stuck, try to ask for help by filing an issue, with a detailed description of what you were trying to do and what went wrong. If you have any questions or if you would like to get involved in our community, please check out:

- [GreptimeDB Community Slack](https://greptime.com/slack)
- [GreptimeDB GitHub Discussions](https://github.com/GreptimeTeam/greptimedb/discussions)

Also, see some extra GreptimeDB content:

- [GreptimeDB Docs](https://docs.greptime.com/)
- [Learn GreptimeDB](https://greptime.com/product/db)
- [Greptime Inc. Website](https://greptime.com)
