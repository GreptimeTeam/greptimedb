## Starts a standalone kafka 
```bash
docker compose -f docker-compose-standalone.yml up kafka -d
```

## Lists running containers
```bash
docker compose -f docker-compose-standalone.yml ps
```

## Stops the standalone kafka 
```bash
docker compose -f docker-compose-standalone.yml stop kafka 
```

## Stops and removes a standalone kafka 
```bash
docker compose -f docker-compose-standalone.yml down kafka 
```