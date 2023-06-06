## Run Chaos on the local machine

### Requires 
- Linux 

### Install
```bash
make install-chaosd
```

### Debug Mode
Debug mode required you setup a debug cluster manually.

You can use the [gtctl](https://github.com/GreptimeTeam/gtctl) to deploy a local cluster on your metal machine, and the sample configuration can be found [here](./hack/debug/basic.yaml).
```bash
make install-gtctl
```

1. Start your debug cluster
```bash
./bin/gtctl cluster create mycluster --bare-metal --config ./hack/debug/basic.yaml
```

2. Start a Chaosd server

```bash
make start-chaosd
```