# kafka-viz

A small local UI for the Kafka CLI you already have installed. It shells out
to the real scripts in `KAFKA_HOME/bin` — no separate Kafka client library,
no separate broker config to maintain.

## Requirements

- Node.js 18+ (on the machine where you'll run this — your laptop, not a
  remote server, unless that server also has network access to your brokers)
- A local Kafka distribution, e.g. `~/Work/kafka_2.13-3.2.1`, with these
  present under `bin/`:
  - `kafka-topics.sh` (list/describe/delete topics)
  - `kafka-console-consumer.sh` (read messages)
  - `kafka-console-producer.sh` (publish messages)
  - `kafka-run-class.sh` (used to fetch partition offsets via `GetOffsetShell`)
  - `kafka-configs.sh` (topic-level config overrides, shown in topic details)
  - `kafka-consumer-groups.sh` (consumer group lag, shown in topic details)
  - `kafka-delete-records.sh` (used by "Purge messages")

  If you don't have one yet, see [Setting up a local Kafka
  distribution](#setting-up-a-local-kafka-distribution) below.
- Network access from this machine to whichever bootstrap servers you add
  (localhost for a local broker, or your QA/prod hosts over VPN, etc.)

## Setting up a local Kafka distribution

### Option A: the setup script

[`scripts/setup-kafka.sh`](scripts/setup-kafka.sh) downloads a release from
`archive.apache.org`, verifies its SHA512 checksum, extracts it, and points
this tool's `config.json` at it:

```bash
npm run setup-kafka                    # installs 3.2.1 (Scala 2.13) into ~/Work
npm run setup-kafka -- --start         # ...and also starts ZooKeeper + the broker
```

Options: `--version`, `--scala`, `--dir` (defaults 3.2.1 / 2.13 / `~/Work`),
`--force` (re-download even if already installed), `--no-update-config`
(don't touch `config.json`), `--help`. Run it again with `--start` any time
to (re)start the pair; each run reuses an existing install unless you pass
`--force`.

### Option B: by hand

If you just want a broker on `localhost:9092` to point this tool at:

1. **Java** — Kafka needs a JRE (11+ recommended). Check with `java -version`;
   install a JDK (e.g. via `brew install openjdk@17` on macOS) if missing.

2. **Download** — grab a binary release from
   [kafka.apache.org/downloads](https://kafka.apache.org/downloads) (the link
   under "Community" on kafka.apache.org). Pick the **Binary downloads**
   section, any Scala build (2.13 is fine) — e.g. `kafka_2.13-3.2.1.tgz`. The
   "Source download" isn't what you want unless you plan to build it yourself.

3. **Extract** it somewhere stable, e.g.:

   ```bash
   mkdir -p ~/Work
   tar -xzf ~/Downloads/kafka_2.13-3.2.1.tgz -C ~/Work
   ```

   This gives you `~/Work/kafka_2.13-3.2.1`, with the CLI scripts under its
   `bin/` — that whole directory is what `kafkaHome` in this tool's Settings
   should point to.

4. **Start ZooKeeper**, then the broker, each in its own terminal, from
   inside the extracted directory (older releases like 3.2.1 need ZooKeeper;
   see the KRaft note below for newer ones):

   ```bash
   # terminal 1 — leave running
   bin/zookeeper-server-start.sh config/zookeeper.properties

   # terminal 2 — leave running, after ZooKeeper has started
   bin/kafka-server-start.sh config/server.properties
   ```

   The broker's default config already listens on `localhost:9092`, matching
   this tool's default `Local` environment.

5. **Verify it's up**:

   ```bash
   bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
   ```

   An empty (but error-free) result means the broker is reachable.

6. Point kafka-viz-tool at it: open the gear icon → set **Kafka home** to
   the extracted directory from step 3, and confirm the `Local` environment's
   bootstrap servers are `localhost:9092`.

To stop, `Ctrl+C` the broker first, then ZooKeeper (the broker depends on it).

<details>
<summary>Using KRaft instead of ZooKeeper (Kafka 3.3+)</summary>

Newer releases can run without ZooKeeper. From the extracted directory:

```bash
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format -t "$KAFKA_CLUSTER_ID" -c config/kraft/server.properties
bin/kafka-server-start.sh config/kraft/server.properties
```

Just one process to run/stop — no separate ZooKeeper step.
</details>

## Setup

```bash
cd kafka-viz-tool
npm install
npm start
```

Then open **http://localhost:4545**.

On first run it creates `config.json` next to `server.js` with:
- `kafkaHome` defaulting to `~/Work/kafka_2.13-3.2.1`
- one environment: `Local` → `localhost:9092`

Open the gear icon (top right) to:
- change `kafkaHome` if your install path is different
- add more environments, e.g.
  - `QA2` → `kafka-01.qa2-sg.cld:9092,kafka-02.qa2-sg.cld:9092`
  - `Prod` → `...`
- remove environments you no longer need

All of this is saved to `config.json` (plain JSON, git-ignored so your real
environments/hostnames never get committed) — edit it by hand if you prefer.
[`config.example.json`](config.example.json) shows the shape if you're
setting this up fresh in a clone.

## Using it

**Topics & messages** — pick an environment, browse/filter topics on the
left, click one to load its most recent messages (last 50, newest first,
merged across all partitions). The header shows how far back the loaded
batch reaches (oldest message's timestamp) so you know the window you're
looking at. Above the message list, filter the *loaded* batch by free-text
search (key/value), partition, key substring, and/or a from/to date range —
none of this requeries Kafka, so it's instant.

Two destructive actions live next to Reload/View details:
- **Purge messages** deletes every record in every partition of the topic
  (via `kafka-delete-records.sh`, deleting up to each partition's current
  latest offset) — the topic and its partitions stay, only the data is gone.
- **Delete topic** removes the topic entirely (`kafka-topics.sh --delete`)
  and requires typing the topic name to confirm.

Kafka has no way to delete a single partition while keeping the topic (the
partition count can only increase), so there's no per-partition delete —
only whole-topic delete or an all-partitions purge.

**Publish** — pick the environment (top right), enter a topic name and a
JSON body, hit Publish. The JSON is validated client- and server-side before
it's piped into `kafka-console-producer.sh`. Key and partition are both
optional:
- **Key** is sent as-is via the console producer's `parse.key`/`key.separator`
  properties.
- **Partition** is populated as a dropdown from the topic's actual partitions
  once you tab out of the Topic field; for a topic that doesn't exist yet (or
  can't be described), it falls back to a manual number input. Since
  `kafka-console-producer.sh` has no flag to target a partition directly, an
  explicit partition (with no key given) is achieved by generating a synthetic
  key whose murmur2 hash Kafka's default partitioner routes to that exact
  partition — the actual key sent is shown in the publish result. If you
  supply your own key *and* a partition, the key wins and the UI warns that
  the partition isn't guaranteed.

## How message loading works

For the selected topic, the backend:
1. runs `GetOffsetShell` to get the latest offset per partition
2. for each partition, computes `start = max(0, latest - limit)`
3. runs `kafka-console-consumer.sh --partition P --offset start --max-messages N --timeout-ms 5000` with timestamp/key printing enabled
4. merges all partitions' messages, sorts by timestamp descending, trims to
   the requested limit

This means very "bursty" multi-partition topics may take a few seconds to
load — it's running one consumer process per partition. Each `kafka-*.sh`
invocation also pays JVM startup cost (~1-3s), which is the dominant part of
the latency you'll see, not network time to the broker. If a topic has many
partitions and you want it faster, lower the limit in `server.js`
(`GET /api/messages` default `limit=50`).

## Notes / limitations

- This is intentionally CLI-wrapper-simple: no persistent consumer groups,
  no schema registry / Avro support, no ACL or SASL/SSL config beyond
  whatever your `kafka-console-*` scripts pick up from their own defaults.
  If your brokers need SASL/SSL, add a `client.properties` and extend the
  `spawn(...)` calls in `server.js` with `--command-config <path>`.
- Message search/filtering is client-side over the currently loaded batch,
  not a full-topic search.
- Explicit partition targeting on publish is best-effort: it works by
  crafting a key that hashes to the desired partition under Kafka's default
  partitioner, so it only applies when you don't also supply your own key.
