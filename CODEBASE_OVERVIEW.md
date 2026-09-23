# kafka-viz-tool — Codebase Overview

## What it is
A local, single-page Kafka UI that shells out to the real CLI scripts in `KAFKA_HOME/bin`
(`kafka-topics.sh`, `kafka-console-consumer.sh`, `kafka-console-producer.sh`,
`kafka-run-class.sh`, `kafka-configs.sh`, `kafka-consumer-groups.sh`) instead of using a
Kafka client library. No database — all state is either in `config.json` or fetched live
from the brokers on demand.

## Stack
- **Backend**: [server.js](server.js) — single-file Express app (Node 18+), no framework beyond Express.
- **Frontend**: [public/index.html](public/index.html) + [public/app.js](public/app.js) + [public/style.css](public/style.css) — vanilla JS/HTML/CSS, no build step, no framework.
- **Config**: [config.json](config.json) — plain JSON, holds `kafkaHome` path and named environments (bootstrap servers).

## Pages / Tabs
The UI is a single page with two tab sections (`public/index.html`), switched client-side:

1. **Topics & messages** (default tab)
2. **Publish**

Plus two modals layered on top of either tab:
- **Settings modal** (gear icon, top right) — manage Kafka home path & environments
- **Topic details modal** — opened via "View details" from the Topics page

## Features by area

### Top bar (global, all pages)
- Environment selector dropdown (switches active bootstrap-servers)
- Light/dark theme toggle (sun/moon icon) — defaults to OS preference, persisted in `localStorage`
- Settings gear icon → opens Settings modal

### Topics & Messages page
- **Topic list (left pane)**: lists all topics for the selected environment, client-side filter box, manual refresh button, and a "+" button to create a new topic (name, partitions, replication factor via `kafka-topics.sh --create`) — the new topic is auto-selected once created. The pane is resizable via a drag divider on its right edge (width persisted in `localStorage`). Deleting a topic clears the filter box and refreshes the list.
- **Message stream (center)**: on selecting a topic, loads its most recent messages (default limit 50) merged and sorted newest-first across *all* partitions. "Load older messages" (shown when more is available) pages backward per partition using a cursor (`nextCursor`/`hasMore` in the response) rather than a fixed count, appending to the currently displayed batch
  - Header shows "loaded until" timestamp — the oldest message's timestamp in the loaded batch, so the user knows the time window covered
  - Client-side search box filters the already-loaded messages by key/value text (no re-query)
  - Reload button re-fetches the batch
  - "View details" button opens the Topic Details modal
  - "Publish message" button switches to the Publish page with the selected topic pre-filled and its partition dropdown populated
  - Filters (all client-side over the loaded batch): partition dropdown, key substring, date range, plus the free-text search
  - "Purge messages" deletes all records in every partition (topic/partitions remain); "Delete topic" removes the topic entirely. Both require typing the topic name into a confirmation modal, and both show a blocking full-page overlay (disabling every other control) while the request is in flight
  - Internal Kafka topics (name starts with `__`, e.g. `__consumer_offsets`) get an "internal" badge in the topic list, and both buttons stay disabled when one is selected - enforced client-side (`isInternalTopic` in app.js) and again server-side (same check in server.js on the delete/purge routes) so it can't be bypassed via a direct API call
- **Message detail pane (right)**: clicking a message in the stream shows its full payload, pretty-printed if it parses as JSON. A toolbar above it has a search box that highlights matching text within the payload (client-side, `<mark>` wrapping - doesn't touch the underlying content), and a copy button that copies the full displayed payload to the clipboard (Clipboard API with an `execCommand('copy')` fallback for contexts where it's unavailable)

### Publish page
- Topic name input — can be typed directly, or auto-filled by the "Publish message" button from the Topics page
- Key (optional) — sent as-is via the console producer's key properties
- Partition (optional) — a dropdown populated from the topic's real partitions once you tab out of the Topic field (or auto-populated when arriving via "Publish message"); falls back to a manual number input for a topic that doesn't exist yet or can't be described. Since `kafka-console-producer.sh` has no direct partition flag, an explicit partition with no key is achieved by generating a synthetic key whose murmur2 hash routes to that partition under Kafka's default partitioner
- JSON message textarea with live JSON validation status
- Publish button — sends via `kafka-console-producer.sh`
- Result/error feedback area, including the actual key used when one was synthesized for partition targeting

### Settings modal
- Edit `kafkaHome` path (persisted to `config.json`)
- List/add/remove named environments (name + comma-separated bootstrap servers)

### Topic Details modal
Populated by `GET /api/topics/:topic/details`, shows:
- Partition count & replication factor
- Per-partition table: leader, replicas, ISR, earliest/latest offset, computed message count
- Total message count across all partitions
- Consumer groups reading this topic: group ID, active consumer IDs, per-partition current offset/log-end-offset/lag, total lag
- Topic-level dynamic config overrides (from `kafka-configs.sh --describe`)

## Backend API (server.js)
| Method | Path | Purpose |
|---|---|---|
| GET | `/api/config` | Return current config (kafkaHome + environments) |
| POST | `/api/config/kafka-home` | Update kafkaHome path |
| POST | `/api/environments` | Add or update an environment |
| DELETE | `/api/environments/:name` | Remove an environment |
| GET | `/api/topics?env=` | List topics (`kafka-topics.sh --list`) |
| POST | `/api/topics` | Create a topic with explicit partitions/replication factor (`kafka-topics.sh --create`) |
| DELETE | `/api/topics/:topic?env=` | Delete a topic entirely (rejects internal topics, name starting with `__`) |
| POST | `/api/topics/:topic/purge?env=` | Delete all records in every partition (`kafka-delete-records.sh`); rejects internal topics |
| GET | `/api/topics/:topic/partitions?env=` | List a topic's partitions (used by Publish's partition picker) |
| GET | `/api/messages?env=&topic=&limit=&since=&cursor=` | Fetch messages across all partitions, newest first. `cursor` (JSON `{partition: exclusiveEndOffset}`, from a prior response's `nextCursor`) continues an older page for "Load older messages"; response includes `nextCursor` and `hasMore` for pagination. `since` (epoch ms) also accepts a time-bounded load via `GetOffsetShell --time` (capped at 2,000 messages), though no UI control currently calls it |
| GET | `/api/topics/:topic/details?env=` | Partition info, offsets, consumer groups, config |
| POST | `/api/publish` | Publish one JSON message to a topic |

### Message-loading algorithm
1. `GetOffsetShell` fetches latest offset per partition
2. For each partition, the fetch range's end (`rangeEnd`) is the partition's latest offset by default, or the offset given in `cursor` for that partition when paging backward
3. The range's start offset is either:
   - `rangeEnd - limit` for a plain count-based/pagination load, or
   - the offset returned by `GetOffsetShell --time <since>` for a time-bounded load (a direct index lookup, not a scan) — still floored at `rangeEnd - limit` as a per-partition safety cap
4. Spawn one `kafka-console-consumer.sh` per partition (`--offset start --max-messages N --timeout-ms`)
5. Merge all partitions' results, sort by timestamp descending, trim to `limit`
6. Return each partition's start offset as `nextCursor`, for a subsequent "load older" request to continue from; `hasMore` is true if any partition's start offset is still above 0

Each CLI invocation pays ~1-3s JVM startup cost, so topics with many partitions take longer to load — this is a known/documented limitation.

## Known limitations (per README)
- No persistent consumer groups, no schema registry/Avro support
- No ACL/SASL/SSL config beyond what the CLI scripts pick up by default
- Message search is client-side over the loaded batch only, not a full-topic search
- Publish sends a single record with no key and default partitioning
