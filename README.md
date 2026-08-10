# Kafka DR Switchover with Cluster Linking and CPC Gateway

This demo shows a local disaster recovery flow for Kafka clients that connect through CPC Gateway.

Two single-node Kafka clusters are started with Docker Compose. Clients connect only to the Gateway route on `localhost:19092`, which initially forwards traffic to `kafka-1`. Cluster Linking mirrors `test-topic` from `kafka-1` to `kafka-2`. After simulating a `kafka-1` failure, the mirror topic is failed over on `kafka-2`, the Gateway route is switched to `kafka2-domain`, and the same client commands can be restarted without changing their bootstrap address.

The example intentionally stays small: no Schema Registry, no Schema Linking, no SASL, and no ACL sync. Cluster Linking handles topic and consumer offset replication; CPC Gateway handles the client-facing route switchover.


## Gateway Configuration Example
- The Gateway container proxies both Kafka clusters.
- `kafka-1` is onboarded as `kafka1-domain` through `internal-kafka1-listener`.
- `kafka-2` is onboarded as `kafka2-domain` through `internal-kafka2-listener`.
- The client-facing route is named `switchover-route` and listens on `localhost:19092`.
- The before template points `switchover-route` to `kafka1-domain`.
- The after template points the same route to `kafka2-domain`.
- For local development, the demo uses port-based routing, plaintext listeners, and passthrough authentication.


```yaml
gateway:
    image: "${GATEWAY_IMAGE}"
    container_name: gateway
    external_links:
      - kafka-1
      - kafka-2
    environment:
      GATEWAY_CONFIG: | 
        gateway:
          admin:
            endpoints:
              metrics: true
          streamingDomains:
            - name: kafka1-domain  
              type: kafka
              kafkaCluster:
                name: kafka-cluster-1 
                nodeIdRanges:
                  - name: d
                    start: 1 
                    end: 5
                bootstrapServers:
                  - id: internal-kafka1-listener  
                    endpoint: "kafka-1:44444" 
            - name: kafka2-domain  
              type: kafka
              kafkaCluster:
                name: kafka-cluster-2 
                nodeIdRanges:
                  - name: d
                    start: 1 
                    end: 5
                bootstrapServers:
                  - id: internal-kafka2-listener  
                    endpoint: "kafka-2:22222" 
          routes:
            - name: switchover-route
              endpoint: "localhost:19092" 
              brokerIdentificationStrategy:
                type: port 
              streamingDomain:
                name: kafka1-domain
                bootstrapServerId: internal-kafka1-listener 
              security:
                auth: passthrough
``` 

## How to Get Started
### Prerequisites
- Docker Desktop (or Docker Engine) with Compose v2
- macOS/Linux shell

### What's here
- `kafka-compose.yaml`: spins up two single-node Kafka clusters (kafka-1 and kafka-2)
- `gateway-compose.before.yaml`: Gateway template with `switchover-route` pointing to kafka-1
- `gateway-compose.after.yaml`: Gateway template with `switchover-route` pointing to kafka-2
- `gateway-compose.local.yaml`: local Gateway runtime config, initialized from `gateway-compose.before.yaml` by `start-gateway.sh` and ignored by Git
- `cluster-linking/source-to-destination.properties`: plaintext Cluster Link config for kafka-2 to mirror from kafka-1
- `cluster-linking/consumer-offset-group-filters.json`: consumer group filter for syncing `dr-demo-consumer` offsets
- `start-kafka.sh`: script to start Kafka clusters
- `start-gateway.sh`: script to start Gateway
- `setup-cluster-linking.sh`: creates the source topic, Cluster Link, and mirror topic
- `simulate-primary-failure-and-switchover.sh`: stops kafka-1, fails over the mirror topic, and switches Gateway to kafka-2

### Quick start
1) From this folder, make the script executable (first time only):
```bash
chmod +x ./start-kafka.sh ./start-gateway.sh ./setup-cluster-linking.sh ./simulate-primary-failure-and-switchover.sh
```
2) Bring up 2 Kafka clusters:
```bash
sh ./start-kafka.sh
```
3) Bring up the Gateway 
```bash
sh ./start-gateway.sh
```

These scripts will help:
- export a default `GATEWAY_IMAGE`
- initialize `gateway-compose.local.yaml` from `gateway-compose.before.yaml` if the local file does not exist
- run `docker compose down -v`, then `docker compose up -d` to bring up broker and gateway containers

### Set Up Cluster Linking

Create a destination-side Cluster Link on kafka-2 and a mirror topic for `test-topic`:

```bash
sh ./setup-cluster-linking.sh
```

The link uses `cluster-linking/source-to-destination.properties`:

```properties
bootstrap.servers=kafka-1:44444
consumer.offset.sync.enable=true
consumer.offset.sync.ms=5000
acl.sync.enable=false
```

The consumer offset sync filter is passed separately through `cluster-linking/consumer-offset-group-filters.json` because the Confluent CLI expects the JSON file via `--consumer-group-filters-json-file` when creating the link.

This demo keeps both Kafka clusters plaintext and unauthenticated, so no client, SASL, or Schema Registry configuration is required.

To inspect the link and mirror topic:

```bash
kafka-cluster-links --bootstrap-server localhost:11111 --list --include-topics
kafka-mirrors --bootstrap-server localhost:11111 --describe --links source-to-destination
```

### Run Console Clients with Gateway

You can download the Kafka clients [here](https://kafka.apache.org/downloads) to get your console clients to work with the Gateway container. Console clients are available within the bin directory once you unzip the Kafka binary.

No client authentication config is required in this setup. The Kafka clusters use plaintext listeners, and the Gateway route transparently forwards plaintext client traffic to those listeners.

Since the Switchover Route is available at Gateway's localhost:19092, we need the clients to connect to localhost:19092 to stream data.
`setup-cluster-linking.sh` already creates `test-topic` on kafka-1 before creating the mirror topic on kafka-2, so the clients can start producing and consuming through the Gateway directly.


**Run the producer**
```
while true; do
  echo "Test message at $(date '+%H:%M:%S')"
  sleep 2
done | kafka-console-producer --bootstrap-server localhost:19092 --topic test-topic
```

**Run the consumer** 
``` 
kafka-console-consumer --bootstrap-server localhost:19092 --topic test-topic --group dr-demo-consumer
```

At this point the flow is:

```text
client -> Gateway switchover-route -> kafka1-domain -> kafka-1
                                                   |
                                                   +-> Cluster Link -> kafka-2 mirror topic
```

### Simulate Primary Failure and Switch Over

Stop the producer and consumer first, then simulate an unplanned kafka-1 failure and move the Gateway route to kafka-2:

```bash
sh ./simulate-primary-failure-and-switchover.sh
```

The script performs the DR sequence:
- waits 10 seconds for mirror data and consumer offsets to settle
- stops `kafka-1`
- runs `kafka-mirrors --failover --topics test-topic` on kafka-2
- copies `gateway-compose.after.yaml` to `gateway-compose.local.yaml`
- restarts Gateway

Restart the same producer and consumer commands. Their bootstrap address stays `localhost:19092`, but Gateway now sends traffic to `kafka2-domain`.

For a planned switchover while kafka-1 is still healthy, wait until mirror lag is zero, use `kafka-mirrors --promote --topics test-topic` instead of failover, then apply the after template and restart Gateway:

```bash
kafka-mirrors --bootstrap-server localhost:11111 --promote --topics test-topic
cp gateway-compose.after.yaml gateway-compose.local.yaml
sh ./start-gateway.sh
```

To switch the route back to kafka-1 for a fresh rerun of the demo, restart the Kafka clusters and apply the before template:

```bash
sh ./start-kafka.sh
cp gateway-compose.before.yaml gateway-compose.local.yaml
sh ./start-gateway.sh
```

### Stop / Clean
```bash
docker compose -f gateway-compose.local.yaml down -v
docker compose -f kafka-compose.yaml down -v
```

For full cleanup also:
```bash
rm -f gateway-compose.local.yaml
```


### Notes
- Run every command from this directory so Docker Compose and the helper scripts find the expected files.
- Start Kafka before Gateway. The local Gateway compose file joins the Kafka compose network `cpc-gateway-dr_default`.
- Host ports used by the demo: Gateway route `localhost:19092`, Gateway admin/extra route ports `19093`, `19094`, `9190`, kafka-1 host listener `localhost:33333`, and kafka-2 host listener `localhost:11111`.
- `gateway-compose.local.yaml` is ignored by Git. The helper scripts create or replace it from `gateway-compose.before.yaml` and `gateway-compose.after.yaml`.
- Cluster Linking is configured outside CPC Gateway. Gateway does not replicate data; it only changes where client traffic is routed.
- The simulated outage uses `kafka-mirrors --failover` because `kafka-1` is stopped before cutover. For a planned switchover while `kafka-1` is healthy and mirror lag is zero, use `kafka-mirrors --promote`.
- This is a local DR demonstration, not a production topology. Production designs should plan security, ACLs, client identity parity, monitoring, failback, and the documented reverse-link or restore workflow.
