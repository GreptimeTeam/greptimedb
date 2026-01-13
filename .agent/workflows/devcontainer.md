---
description: How to execute commands in the devcontainer
---

# DevContainer Command Execution

All cargo/rust related commands should be executed inside the devcontainer for this project.

## Command Pattern

// turbo-all

To execute any command in the devcontainer, use:

```bash
devcontainer exec --workspace-folder . <command>
```

Note: Run this from the project root directory (wherever the `.devcontainer` folder is located).

## Examples

1. Run cargo check:
```bash
devcontainer exec --workspace-folder . cargo check --workspace
```

2. Run cargo build:
```bash
devcontainer exec --workspace-folder . cargo build
```

3. Run tests:
```bash
devcontainer exec --workspace-folder . cargo nextest run
```

4. Run clippy:
```bash
devcontainer exec --workspace-folder . cargo clippy --all-targets
```

5. Format code:
```bash
devcontainer exec --workspace-folder . cargo fmt --all
```

6. Interactive bash shell:
```bash
devcontainer exec --workspace-folder . bash
```

## Starting the Container

Before running any commands, ensure the devcontainer is running. Use this command which will start the container if not running, or do nothing if already running:

```bash
devcontainer up --workspace-folder . 2>/dev/null || devcontainer up --workspace-folder .
```

## Error Handling

If `devcontainer exec` fails:

1. **Container not running**: Run `devcontainer up --workspace-folder .` first
2. **Container needs rebuild**: Run `devcontainer up --workspace-folder . --remove-existing-container --build-no-cache`
3. **Check container logs**: Run `docker logs <container_id>` to diagnose issues+

## Important Notes

- Run all commands from the project root directory (where `.devcontainer` folder exists)
- The workspace is mounted at `/workspace` inside the container
- Cargo caches are shared from the host's `~/.cargo`
- The target directory is shared, so builds are persistent
- The container will auto-start if you run `devcontainer exec` and it's not running