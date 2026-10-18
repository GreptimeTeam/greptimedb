# Correctness results of run records-20261018T091334Z

This run is the ordinary plan against the fused path on one build (`a2d345cc76c`), which is
section 4.7 of `docs/fused-series-report.md`. Each round runs one dedicated correctness query
per arm and compares the two through `fused_series_bench.py compare`. The full outputs are
~300 KB each and are not committed; this manifest lets a reviewer check that a copy of a
result file is the one that was compared.

| result | rows | sha256 |
| --- | ---: | --- |
| `cC-r1-F-C.json` | 480 | `de62026055bb001d275e98f9548121d7` |
| `cC-r1-P-C.json` | 480 | `2c615b3737e9635d2349ebd4350e1763` |
| `cC-r2-F-C.json` | 480 | `6220720e9c5ec8bfc40e5e92395aa317` |
| `cC-r2-P-C.json` | 480 | `901316f651c771701b7c17c23b2916ed` |
| `cC-r3-F-C.json` | 480 | `c1a7179a0eb308b3cd84e169e0325e03` |
| `cC-r3-P-C.json` | 480 | `c07b5a16dd90671f798f93e6c2d93fcd` |
| `cG-r1-F-G.json` | 28684 | `05a9fad60d83b6b3d3fcb580a21ba99d` |
| `cG-r1-P-G.json` | 28684 | `02332b63609752147f763a6b0cc05beb` |
| `cG-r2-F-G.json` | 28684 | `f70622032cfe46b2c7cc5f548a267e63` |
| `cG-r2-P-G.json` | 28684 | `c553f0aae1a14fd0f3cd590e9c781699` |
| `cG-r3-F-G.json` | 28684 | `b75eddb463600b4b676df687db55d352` |
| `cG-r3-P-G.json` | 28684 | `1144d8b906366f5cbf3f91120c7d5333` |
| `cS-r1-F-S.json` | 7084 | `cdd269fd5c8e5af93db542e82e3bf3be` |
| `cS-r1-P-S.json` | 7084 | `fd174661638bf2769087ff05adbf53b3` |
| `cS-r2-F-S.json` | 7084 | `74a1f6bb17081c7432848734dfbdb3dc` |
| `cS-r2-P-S.json` | 7084 | `8d21272236e6410ae101ec5be782228e` |
| `cS-r3-F-S.json` | 7084 | `e9232a0f72fdc5e34bfe24277752f1bd` |
| `cS-r3-P-S.json` | 7084 | `472d1ec7840c140699802368a7921876` |
| `cW-r1-F-W.json` | 28684 | `e0e025e02fa90b6406101bcef2bb8f48` |
| `cW-r1-P-W.json` | 28684 | `ef5210ca2d4677a647c17a4320f30a77` |
| `cW-r2-F-W.json` | 28684 | `2898935507092729f84e98d3d4049426` |
| `cW-r2-P-W.json` | 28684 | `e2484adec35dfef6822132c2f6122a50` |
| `cW-r3-F-W.json` | 28684 | `36f4128b751cf64641f620e2d8cc8ac1` |
| `cW-r3-P-W.json` | 28684 | `7055cce0ad51b5bfb597f550e174c722` |
| `ccontrol-r1-F-control.json` | 28744 | `858fbbad99a1e774716c14d0ddf02189` |
| `ccontrol-r1-P-control.json` | 28744 | `41ab1014fd3df01fceed4952f731ac8c` |
| `ccontrol-r2-F-control.json` | 28744 | `858fbbad99a1e774716c14d0ddf02189` |
| `ccontrol-r2-P-control.json` | 28744 | `41ab1014fd3df01fceed4952f731ac8c` |
| `ccontrol-r3-F-control.json` | 28744 | `858fbbad99a1e774716c14d0ddf02189` |
| `ccontrol-r3-P-control.json` | 28744 | `41ab1014fd3df01fceed4952f731ac8c` |
