# kafka-practice

A small, hands-on playground for learning [Apache Kafka](https://kafka.apache.org/)
with Python. It spins up a single-broker Kafka cluster with Docker and includes a
producer and two consumers that demonstrate **keyed partitioning** and
**consumer-group load balancing**.

## What it demonstrates

- **Keyed messages** — the producer sends events keyed by user id. Kafka hashes
  the key to choose a partition, so every event for a given user always lands on
  the same partition (and is therefore consumed in order).
- **Partitioning** — the topic `my-topic` is auto-created with 3 partitions.
- **Consumer groups** — `consumer.py` and `consumer2.py` share one `group_id`, so
  Kafka splits the 3 partitions between them. Run both to watch the partitions get
  rebalanced across the two consumers.

## Requirements

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose
- Python 3.10+

## Setup

Start the Kafka broker (and Zookeeper):

```bash
docker compose up -d
```

Create a virtual environment and install the Python dependency:

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Usage

In one terminal, start a consumer:

```bash
python consumer.py
```

Optionally start the second consumer in another terminal to see the partitions
shared across the group:

```bash
python consumer2.py
```

Then, in a third terminal, run the producer to send messages:

```bash
python producer.py
```

The producer sends a `login`, `purchase`, and `logout` event for each of
`user-1` through `user-10`. Watch the consumer terminals print which partition
and offset each message arrived on.

## Cleanup

Stop and remove the containers when you're done:

```bash
docker compose down
```

## Files

| File                 | Description                                              |
| -------------------- | -------------------------------------------------------- |
| `docker-compose.yml` | Single-broker Kafka + Zookeeper, topic auto-created with 3 partitions |
| `producer.py`        | Sends JSON events keyed by user id                       |
| `consumer.py`        | Reads from `my-topic` in consumer group `my-consumer-group` |
| `consumer2.py`       | Second consumer in the same group, to demonstrate rebalancing |

## License

Released under the [MIT License](LICENSE).
