# Change Log Level on the Fly

## HTTP API

example:
```bash
> curl --data "trace;flow=debug" 127.0.0.1:4000/debug/log_level;
Log Level changed from Some("info") to "trace"%
```
The data is a string in the format of `module1=level1;module2=level2;...` that follow the same rule of `RUST_LOG`. The module is the module name of the log, and the level is the log level. The log level can be one of the following: `trace`, `debug`, `info`, `warn`, `error`, `off`.