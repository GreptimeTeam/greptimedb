---
description: How to execute commands in the devcontainer
---

# DevContainer Command Execution

All cargo/rust related commands should be executed inside the devcontainer for this project.

## Command Pattern

// turbo-all

To execute any command in the devcontainer, use:

```bash
devcontainer exec --workspace-folder /home/discord9/greptimedb <command>
```

## Examples

1. Run cargo check:
```bash
devcontainer exec --workspace-folder /home/discord9/greptimedb cargo check --workspace
```

2. Run cargo build:
```bash
devcontainer exec --workspace-folder /home/discord9/greptimedb cargo build
```

3. Run tests:
```bash
devcontainer exec --workspace-folder /home/discord9/greptimedb cargo nextest run
```

4. Run clippy:
```bash
devcontainer exec --workspace-folder /home/discord9/greptimedb cargo clippy --all-targets
```

5. Format code:
```bash
devcontainer exec --workspace-folder /home/discord9/greptimedb cargo fmt --all
```

6. Interactive bash shell:
```bash
devcontainer exec --workspace-folder /home/discord9/greptimedb bash
```

## Important Notes

- The devcontainer must be running first (`devcontainer up --workspace-folder /home/discord9/greptimedb`)
- The workspace is mounted at `/workspace` inside the container
- Cargo caches are shared from `/home/discord9/.cargo`
- The target directory is shared, so builds are persistent
