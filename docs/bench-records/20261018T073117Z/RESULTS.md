# Correctness results of run records-20261018T073117Z

Each round runs one dedicated correctness query per arm and compares the two through
`fused_series_bench.py compare`. The full outputs are ~300 KB each and are not committed;
this manifest lets a reviewer check that a copy of a result file is the one that was
compared, and that a re-run reproduced the same rows.

| result | rows | sha256 |
| --- | ---: | --- |
| `cC-r1-F-C.json` | 480 | `2e3a67c0fed9a7e31bfd647b22f66757` |
| `cC-r1-P-C.json` | 480 | `5f6cfc4199b46c1b3eec6e2c580f8386` |
| `cC-r2-F-C.json` | 480 | `134bcfb9afb6fbdb5d01735089844170` |
| `cC-r2-P-C.json` | 480 | `c76f856050fdbb5862472604d104f6bd` |
| `cC-r3-F-C.json` | 480 | `1b407b45312a90769ea0c2adb3ab76f8` |
| `cC-r3-P-C.json` | 480 | `033377262e330142c216cce943b4ee62` |
| `cG-r1-F-G.json` | 28684 | `5e39fca12afa2d796c110d60a38a9d3d` |
| `cG-r1-P-G.json` | 28684 | `539a879a71550d75fd38ebdce1b40ef3` |
| `cG-r2-F-G.json` | 28684 | `bfecf5e7916bf160b5341ca526369d3e` |
| `cG-r2-P-G.json` | 28684 | `a6a1ea94f0b2dbc8915b0a50a00fd368` |
| `cG-r3-F-G.json` | 28684 | `5783c2e9cc1d23ffb9323027191d0c6c` |
| `cG-r3-P-G.json` | 28684 | `aba82b62008b726acfd8e8628fe6b170` |
| `cS-r1-F-S.json` | 7084 | `fcc7582541a0e049767aaa43afd276f5` |
| `cS-r1-P-S.json` | 7084 | `3ce4cdb87ebf2c7b961c9049dc69de00` |
| `cS-r2-F-S.json` | 7084 | `3a1aa7935413b59cd68e0755c5fff8e8` |
| `cS-r2-P-S.json` | 7084 | `9a225a44c0fab05ca2d1f69e58aaba23` |
| `cS-r3-F-S.json` | 7084 | `33b0ae2445bb0546bd6da2251165523e` |
| `cS-r3-P-S.json` | 7084 | `de67bad21519ec97213e27de1e871c4a` |
| `cW-r1-F-W.json` | 28684 | `9ed83acf4d01f61f390759fb880b2233` |
| `cW-r1-P-W.json` | 28684 | `6eb9840a957b20ebcacc84afc0f84ccc` |
| `cW-r2-F-W.json` | 28684 | `f46ceeb5b48b77bfbc2b7e7b3a4d12aa` |
| `cW-r2-P-W.json` | 28684 | `14b572d192c5492d927ed77b60a878fe` |
| `cW-r3-F-W.json` | 28684 | `b8bcdc42f2f6d7ee0a01571bf2c1e17e` |
| `cW-r3-P-W.json` | 28684 | `bab7e9bbefa50af6de454c3e65eca978` |
| `ccontrol-r1-F-control.json` | 28744 | `234d6223d05953602d954fd2b1ebad45` |
| `ccontrol-r1-P-control.json` | 28744 | `3cb293cebe7e65bde0996f41859b79bd` |
| `ccontrol-r2-F-control.json` | 28744 | `234d6223d05953602d954fd2b1ebad45` |
| `ccontrol-r2-P-control.json` | 28744 | `3cb293cebe7e65bde0996f41859b79bd` |
| `ccontrol-r3-F-control.json` | 28744 | `234d6223d05953602d954fd2b1ebad45` |
| `ccontrol-r3-P-control.json` | 28744 | `3cb293cebe7e65bde0996f41859b79bd` |
