/**
 * Kafka Visualisation Tool - backend
 *
 * Wraps the real Kafka CLI scripts shipped in KAFKA_HOME/bin so the UI can:
 *   - manage named environments (bootstrap servers)
 *   - list topics
 *   - view recent messages per topic (across all partitions)
 *   - publish a message to a topic
 *
 * This must run on a machine that actually has the Kafka distribution
 * installed and network access to the target brokers.
 */

const express = require('express');
const path = require('path');
const fs = require('fs');
const os = require('os');
const { spawn } = require('child_process');

const app = express();
app.use(express.json({ limit: '2mb' }));
app.use(express.static(path.join(__dirname, 'public')));

const CONFIG_PATH = path.join(__dirname, 'config.json');
const DEFAULT_KAFKA_HOME = path.join(os.homedir(), 'Work', 'kafka_2.13-3.2.1');

// ---------- config persistence ----------

function loadConfig() {
  if (!fs.existsSync(CONFIG_PATH)) {
    const initial = {
      kafkaHome: DEFAULT_KAFKA_HOME,
      environments: [
        { name: 'Local', bootstrapServers: 'localhost:9092' }
      ]
    };
    fs.writeFileSync(CONFIG_PATH, JSON.stringify(initial, null, 2));
    return initial;
  }
  return JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));
}

function saveConfig(cfg) {
  fs.writeFileSync(CONFIG_PATH, JSON.stringify(cfg, null, 2));
}

function kafkaBin(cfg, scriptName) {
  return path.join(cfg.kafkaHome, 'bin', scriptName);
}

// ---------- process helper ----------

/**
 * Run a CLI command, collect stdout/stderr, resolve on exit (never rejects on
 * non-zero exit so callers can inspect stderr for a useful message).
 */
function runCommand(cmd, args, { timeoutMs = 20000, input = null } = {}) {
  return new Promise((resolve) => {
    const child = spawn(cmd, args, { shell: false });
    let stdout = '';
    let stderr = '';
    let settled = false;

    const timer = setTimeout(() => {
      if (!settled) {
        child.kill('SIGTERM');
      }
    }, timeoutMs);

    child.stdout.on('data', (d) => { stdout += d.toString(); });
    child.stderr.on('data', (d) => { stderr += d.toString(); });

    child.on('error', (err) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve({ code: -1, stdout, stderr: stderr + '\n' + err.message });
    });

    child.on('close', (code) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve({ code, stdout, stderr });
    });

    if (input !== null) {
      child.stdin.write(input);
    }
    child.stdin.end();
  });
}

function checkKafkaHome(cfg, res) {
  const script = kafkaBin(cfg, 'kafka-topics.sh');
  if (!fs.existsSync(script)) {
    res.status(400).json({
      error: `Kafka scripts not found at ${script}. Check the Kafka home path in Settings.`
    });
    return false;
  }
  return true;
}

function getEnv(cfg, name) {
  return cfg.environments.find((e) => e.name === name);
}

// ---------- config / environment endpoints ----------

app.get('/api/config', (req, res) => {
  const cfg = loadConfig();
  res.json(cfg);
});

app.post('/api/config/kafka-home', (req, res) => {
  const { kafkaHome } = req.body;
  if (!kafkaHome || typeof kafkaHome !== 'string') {
    return res.status(400).json({ error: 'kafkaHome is required' });
  }
  const cfg = loadConfig();
  cfg.kafkaHome = kafkaHome;
  saveConfig(cfg);
  res.json(cfg);
});

app.post('/api/environments', (req, res) => {
  const { name, bootstrapServers } = req.body;
  if (!name || !bootstrapServers) {
    return res.status(400).json({ error: 'name and bootstrapServers are required' });
  }
  const cfg = loadConfig();
  const existing = getEnv(cfg, name);
  if (existing) {
    existing.bootstrapServers = bootstrapServers;
  } else {
    cfg.environments.push({ name, bootstrapServers });
  }
  saveConfig(cfg);
  res.json(cfg);
});

app.delete('/api/environments/:name', (req, res) => {
  const cfg = loadConfig();
  cfg.environments = cfg.environments.filter((e) => e.name !== req.params.name);
  saveConfig(cfg);
  res.json(cfg);
});

// ---------- topics ----------

app.get('/api/topics', async (req, res) => {
  const cfg = loadConfig();
  if (!checkKafkaHome(cfg, res)) return;

  const envName = req.query.env;
  const env = getEnv(cfg, envName);
  if (!env) return res.status(400).json({ error: `Unknown environment: ${envName}` });

  const { code, stdout, stderr } = await runCommand(
    kafkaBin(cfg, 'kafka-topics.sh'),
    ['--bootstrap-server', env.bootstrapServers, '--list'],
    { timeoutMs: 15000 }
  );

  if (code !== 0) {
    return res.status(500).json({ error: stderr || 'Failed to list topics' });
  }

  const topics = stdout
    .split('\n')
    .map((l) => l.trim())
    .filter(Boolean)
    .sort();

  res.json({ topics });
});

app.delete('/api/topics/:topic', async (req, res) => {
  const cfg = loadConfig();
  if (!checkKafkaHome(cfg, res)) return;

  const envName = req.query.env;
  const topic = req.params.topic;
  const env = getEnv(cfg, envName);
  if (!env) return res.status(400).json({ error: `Unknown environment: ${envName}` });

  const { code, stderr } = await runCommand(
    kafkaBin(cfg, 'kafka-topics.sh'),
    ['--bootstrap-server', env.bootstrapServers, '--delete', '--topic', topic],
    { timeoutMs: 20000 }
  );

  if (code !== 0) {
    return res.status(500).json({ error: stderr || 'Failed to delete topic' });
  }

  res.json({ ok: true });
});

// Purges every partition of a topic by deleting all records up to each
// partition's current latest offset (kafka-delete-records.sh). The topic and
// its partitions remain - only the records are gone. Kafka has no way to
// remove a single partition from a topic, so this always covers all of them.
app.post('/api/topics/:topic/purge', async (req, res) => {
  const cfg = loadConfig();
  if (!checkKafkaHome(cfg, res)) return;

  const envName = req.query.env;
  const topic = req.params.topic;
  const env = getEnv(cfg, envName);
  if (!env) return res.status(400).json({ error: `Unknown environment: ${envName}` });

  let offsetFile = null;
  try {
    const partitions = await getLatestOffsets(cfg, env, topic);
    if (!partitions.length) {
      return res.status(400).json({ error: 'Topic has no partitions (or does not exist)' });
    }

    const spec = {
      partitions: partitions.map((p) => ({ topic, partition: p.partition, offset: p.latestOffset })),
      version: 1
    };
    offsetFile = path.join(os.tmpdir(), `kvt-purge-${Date.now()}-${Math.random().toString(36).slice(2)}.json`);
    fs.writeFileSync(offsetFile, JSON.stringify(spec));

    const { code, stdout, stderr } = await runCommand(
      kafkaBin(cfg, 'kafka-delete-records.sh'),
      ['--bootstrap-server', env.bootstrapServers, '--offset-json-file', offsetFile],
      { timeoutMs: 20000 }
    );

    if (code !== 0) {
      return res.status(500).json({ error: stderr || 'Failed to purge records' });
    }

    const warning = /error:/i.test(stdout)
      ? 'Some partitions reported an error while purging - check server logs.'
      : null;
    if (warning) console.error(`[purge ${topic}]`, stdout);

    res.json({ ok: true, partitionCount: partitions.length, warning });
  } catch (err) {
    res.status(500).json({ error: err.message });
  } finally {
    if (offsetFile) fs.unlink(offsetFile, () => {});
  }
});

// ---------- messages ----------

// time: -1 = latest (high watermark), -2 = earliest available offset
async function getOffsetsAtTime(cfg, env, topic, time) {
  const { code, stdout, stderr } = await runCommand(
    kafkaBin(cfg, 'kafka-run-class.sh'),
    [
      'kafka.tools.GetOffsetShell',
      '--broker-list', env.bootstrapServers,
      '--topic', topic,
      '--time', String(time)
    ],
    { timeoutMs: 15000 }
  );

  if (code !== 0) {
    throw new Error(stderr || 'Failed to fetch partition offsets');
  }

  // lines look like: "<topic>:<partition>:<offset>"
  return stdout
    .split('\n')
    .map((l) => l.trim())
    .filter(Boolean)
    .map((line) => {
      const parts = line.split(':');
      const offset = parts.pop();
      const partition = parts.pop();
      return { partition: Number(partition), offset: Number(offset) };
    })
    .filter((p) => Number.isFinite(p.partition) && Number.isFinite(p.offset));
}

async function getLatestOffsets(cfg, env, topic) {
  const offsets = await getOffsetsAtTime(cfg, env, topic, -1);
  return offsets.map((o) => ({ partition: o.partition, latestOffset: o.offset }));
}

const KEY_SEP = '\u0001';

function parseConsumerLine(line) {
  // kafka's DefaultMessageFormatter uses `key.separator` between EVERY
  // printed field, not just key/value - so with print.timestamp=true and
  // print.key=true the line looks like:
  //   CreateTime:<ts><KEY_SEP><key-or-"null"><KEY_SEP><value>
  const parts = line.split(KEY_SEP);
  if (parts.length < 3) return null; // not a data line (e.g. a status/log line)

  const tsMatch = parts[0].match(/^(CreateTime|LogAppendTime):(-?\d+)$/);
  if (!tsMatch) return null;

  const timestamp = Number(tsMatch[2]);
  const rawKey = parts[1];
  const key = rawKey === 'null' ? null : rawKey;
  const value = parts.slice(2).join(KEY_SEP); // rejoin in case value contained the separator

  return { timestamp, key, value };
}

async function fetchRecentMessagesForPartition(cfg, env, topic, partition, latestOffset, limit) {
  const startOffset = Math.max(0, latestOffset - limit);
  const countToFetch = latestOffset - startOffset;
  if (countToFetch <= 0) return [];

  const { stdout } = await runCommand(
    kafkaBin(cfg, 'kafka-console-consumer.sh'),
    [
      '--bootstrap-server', env.bootstrapServers,
      '--topic', topic,
      '--partition', String(partition),
      '--offset', String(startOffset),
      '--max-messages', String(countToFetch),
      '--timeout-ms', '5000', // consumer exits as soon as max-messages is hit; this only
                              // bounds the worst case (fewer messages available than expected)
      '--property', 'print.timestamp=true',
      '--property', 'print.key=true',
      '--property', `key.separator=${KEY_SEP}`
    ],
    { timeoutMs: 8000 }
  );

  const lines = stdout.split('\n').filter(Boolean);
  const messages = [];
  let offset = startOffset;
  for (const line of lines) {
    const parsed = parseConsumerLine(line);
    if (parsed) {
      messages.push({ ...parsed, partition, offset });
      offset += 1;
    }
  }
  return messages;
}

app.get('/api/messages', async (req, res) => {
  const cfg = loadConfig();
  if (!checkKafkaHome(cfg, res)) return;

  const envName = req.query.env;
  const topic = req.query.topic;
  const limit = Math.min(Number(req.query.limit) || 50, 500);

  const env = getEnv(cfg, envName);
  if (!env) return res.status(400).json({ error: `Unknown environment: ${envName}` });
  if (!topic) return res.status(400).json({ error: 'topic is required' });

  try {
    const partitions = await getLatestOffsets(cfg, env, topic);
    const perPartitionLimit = limit; // fetch up to `limit` from each, then merge+trim

    const results = await Promise.all(
      partitions.map((p) =>
        fetchRecentMessagesForPartition(cfg, env, topic, p.partition, p.latestOffset, perPartitionLimit)
      )
    );

    let messages = results.flat();
    messages.sort((a, b) => b.timestamp - a.timestamp); // newest first
    messages = messages.slice(0, limit);

    const oldestLoadedTimestamp = messages.length
      ? messages[messages.length - 1].timestamp
      : null;

    res.json({
      messages,
      loadedUntil: oldestLoadedTimestamp, // ms epoch; oldest message in this loaded batch
      partitionCount: partitions.length
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ---------- topic details ----------

async function getTopicPartitionsInfo(cfg, env, topic) {
  const { code, stdout, stderr } = await runCommand(
    kafkaBin(cfg, 'kafka-topics.sh'),
    ['--bootstrap-server', env.bootstrapServers, '--describe', '--topic', topic],
    { timeoutMs: 15000 }
  );
  if (code !== 0) throw new Error(stderr || 'Failed to describe topic');

  const partitionCountMatch = stdout.match(/PartitionCount:\s*(\d+)/);
  const replicationFactorMatch = stdout.match(/ReplicationFactor:\s*(\d+)/);

  const partitions = [];
  const partitionLineRe = /Partition:\s*(\d+)\s+Leader:\s*(-?\d+)\s+Replicas:\s*([\d,]+)\s+Isr:\s*([\d,]+)/g;
  let m;
  while ((m = partitionLineRe.exec(stdout)) !== null) {
    partitions.push({
      partition: Number(m[1]),
      leader: Number(m[2]),
      replicas: m[3].split(',').map(Number),
      isr: m[4].split(',').map(Number)
    });
  }
  partitions.sort((a, b) => a.partition - b.partition);

  return {
    partitionCount: partitionCountMatch ? Number(partitionCountMatch[1]) : partitions.length,
    replicationFactor: replicationFactorMatch
      ? Number(replicationFactorMatch[1])
      : (partitions[0] ? partitions[0].replicas.length : null),
    partitions
  };
}

// kafka-consumer-groups.sh --describe --all-groups columns (with --all-groups, GROUP is included):
//   GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID
function parseConsumerGroupsForTopic(stdout, topic) {
  const lines = stdout.split('\n').map((l) => l.trim()).filter(Boolean);
  const groups = new Map();

  for (const line of lines) {
    if (line.startsWith('GROUP ') || line.startsWith('GROUP\t')) continue; // header
    if (/^(Note|Warning):/.test(line)) continue;
    if (line.includes('has no active members') || line.includes('is rebalancing')) continue;

    const cols = line.split(/\s+/);
    if (cols.length < 9) continue;
    const [groupId, topicCol, partitionStr, currentOffsetStr, logEndOffsetStr, lagStr, consumerId, host, clientId] = cols;
    if (topicCol !== topic) continue;

    if (!groups.has(groupId)) groups.set(groupId, { groupId, partitions: [], consumerIds: new Set() });
    const g = groups.get(groupId);
    const active = consumerId !== '-';

    g.partitions.push({
      partition: Number(partitionStr),
      currentOffset: currentOffsetStr === '-' ? null : Number(currentOffsetStr),
      logEndOffset: logEndOffsetStr === '-' ? null : Number(logEndOffsetStr),
      lag: lagStr === '-' ? null : Number(lagStr),
      consumerId: active ? consumerId : null,
      host: active ? host : null,
      clientId: active ? clientId : null
    });
    if (active) g.consumerIds.add(consumerId);
  }

  return Array.from(groups.values()).map((g) => ({
    groupId: g.groupId,
    consumerIds: Array.from(g.consumerIds),
    totalLag: g.partitions.reduce((sum, p) => sum + (Number.isFinite(p.lag) ? p.lag : 0), 0),
    partitions: g.partitions.sort((a, b) => a.partition - b.partition)
  }));
}

// Best-effort: only returns dynamic config overrides (topic-level), not full effective config.
async function getTopicConfig(cfg, env, topic) {
  const { code, stdout } = await runCommand(
    kafkaBin(cfg, 'kafka-configs.sh'),
    ['--bootstrap-server', env.bootstrapServers, '--describe', '--entity-type', 'topics', '--entity-name', topic],
    { timeoutMs: 15000 }
  );
  if (code !== 0) return {};

  const configs = {};
  const re = /([\w.]+)=([^\s]+)\s+sensitive=/g;
  let m;
  while ((m = re.exec(stdout)) !== null) {
    configs[m[1]] = m[2];
  }
  return configs;
}

app.get('/api/topics/:topic/partitions', async (req, res) => {
  const cfg = loadConfig();
  if (!checkKafkaHome(cfg, res)) return;

  const envName = req.query.env;
  const topic = req.params.topic;
  const env = getEnv(cfg, envName);
  if (!env) return res.status(400).json({ error: `Unknown environment: ${envName}` });

  try {
    const info = await getTopicPartitionsInfo(cfg, env, topic);
    res.json({ partitionCount: info.partitionCount, partitions: info.partitions.map((p) => p.partition) });
  } catch (err) {
    res.status(404).json({ error: err.message });
  }
});

app.get('/api/topics/:topic/details', async (req, res) => {
  const cfg = loadConfig();
  if (!checkKafkaHome(cfg, res)) return;

  const envName = req.query.env;
  const topic = req.params.topic;
  const env = getEnv(cfg, envName);
  if (!env) return res.status(400).json({ error: `Unknown environment: ${envName}` });

  try {
    const [topicInfo, latestOffsets, earliestOffsets, groupsResult, config] = await Promise.all([
      getTopicPartitionsInfo(cfg, env, topic),
      getOffsetsAtTime(cfg, env, topic, -1),
      getOffsetsAtTime(cfg, env, topic, -2),
      runCommand(
        kafkaBin(cfg, 'kafka-consumer-groups.sh'),
        ['--bootstrap-server', env.bootstrapServers, '--describe', '--all-groups'],
        { timeoutMs: 45000 }
      ),
      getTopicConfig(cfg, env, topic)
    ]);

    const latestMap = new Map(latestOffsets.map((o) => [o.partition, o.offset]));
    const earliestMap = new Map(earliestOffsets.map((o) => [o.partition, o.offset]));

    const partitions = topicInfo.partitions.map((p) => {
      const latest = latestMap.has(p.partition) ? latestMap.get(p.partition) : null;
      const earliest = earliestMap.has(p.partition) ? earliestMap.get(p.partition) : null;
      return {
        ...p,
        earliestOffset: earliest,
        latestOffset: latest,
        messageCount: (latest !== null && earliest !== null) ? latest - earliest : null
      };
    });

    const totalMessages = partitions.reduce((sum, p) => sum + (p.messageCount || 0), 0);
    const consumerGroups = parseConsumerGroupsForTopic(groupsResult.stdout || '', topic);

    res.json({
      partitionCount: topicInfo.partitionCount,
      replicationFactor: topicInfo.replicationFactor,
      partitions,
      totalMessages,
      consumerGroups,
      config
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ---------- publish ----------

// Mirrors org.apache.kafka.common.utils.Utils.murmur2 - the hash Kafka's
// DefaultPartitioner uses to route a keyed record to a partition
// (partition = toPositive(murmur2(keyBytes)) % numPartitions). We need the
// exact same hash to reverse-engineer a key that lands on a chosen partition.
function murmur2(bytes) {
  const m = 0x5bd1e995;
  const r = 24;
  const length4 = Math.floor(bytes.length / 4);

  let h = (0x9747b28c ^ bytes.length) | 0;

  for (let i = 0; i < length4; i++) {
    const i4 = i * 4;
    let k = (bytes[i4] & 0xff) |
            ((bytes[i4 + 1] & 0xff) << 8) |
            ((bytes[i4 + 2] & 0xff) << 16) |
            ((bytes[i4 + 3] & 0xff) << 24);
    k = Math.imul(k, m);
    k ^= k >>> r;
    k = Math.imul(k, m);
    h = Math.imul(h, m);
    h ^= k;
  }

  const rem = bytes.length % 4;
  const tailStart = bytes.length - rem;
  if (rem === 3) h ^= (bytes[tailStart + 2] & 0xff) << 16;
  if (rem >= 2) h ^= (bytes[tailStart + 1] & 0xff) << 8;
  if (rem >= 1) {
    h ^= (bytes[tailStart] & 0xff);
    h = Math.imul(h, m);
  }

  h ^= h >>> 13;
  h = Math.imul(h, m);
  h ^= h >>> 15;

  return h | 0;
}

function toPositive(n) {
  return n & 0x7fffffff;
}

function partitionForKey(key, numPartitions) {
  return toPositive(murmur2(Buffer.from(key, 'utf8'))) % numPartitions;
}

// Best-effort: kafka-console-producer.sh has no way to target a partition
// directly, so when the caller wants an explicit partition (and hasn't
// supplied their own key) we synthesize a key whose murmur2 hash happens to
// route there under Kafka's default partitioner.
function findKeyForPartition(targetPartition, numPartitions) {
  for (let i = 0; i < 100000; i++) {
    const candidate = `kv-${i}`;
    if (partitionForKey(candidate, numPartitions) === targetPartition) return candidate;
  }
  throw new Error('Could not generate a key that maps to that partition');
}

app.post('/api/publish', async (req, res) => {
  const cfg = loadConfig();
  if (!checkKafkaHome(cfg, res)) return;

  const { env: envName, topic, message, key, partition } = req.body;
  const env = getEnv(cfg, envName);
  if (!env) return res.status(400).json({ error: `Unknown environment: ${envName}` });
  if (!topic) return res.status(400).json({ error: 'topic is required' });
  if (message === undefined || message === null || message === '') {
    return res.status(400).json({ error: 'message is required' });
  }

  // Validate JSON, but send the compact single-line form so the console
  // producer (which is newline-delimited) treats it as exactly one record.
  let payload;
  try {
    const parsed = typeof message === 'string' ? JSON.parse(message) : message;
    payload = JSON.stringify(parsed);
  } catch (e) {
    return res.status(400).json({ error: 'Message is not valid JSON: ' + e.message });
  }

  const hasKey = typeof key === 'string' && key.length > 0;
  const hasPartition = partition !== undefined && partition !== null && partition !== '';

  let effectiveKey = hasKey ? key : null;
  let warning = null;

  if (hasPartition) {
    const partitionNum = Number(partition);
    if (!Number.isInteger(partitionNum) || partitionNum < 0) {
      return res.status(400).json({ error: 'partition must be a non-negative integer' });
    }
    if (hasKey) {
      warning = `A key was provided, so the requested partition (${partitionNum}) is not guaranteed - Kafka routes keyed records by the key's hash.`;
    } else {
      try {
        const topicInfo = await getTopicPartitionsInfo(cfg, env, topic);
        const numPartitions = topicInfo.partitionCount;
        if (!numPartitions || partitionNum >= numPartitions) {
          return res.status(400).json({ error: `Topic has ${numPartitions || 0} partition(s); ${partitionNum} is out of range` });
        }
        effectiveKey = findKeyForPartition(partitionNum, numPartitions);
      } catch (e) {
        return res.status(500).json({ error: `Could not resolve partition count: ${e.message}` });
      }
    }
  }

  const args = ['--bootstrap-server', env.bootstrapServers, '--topic', topic];
  let input;
  if (effectiveKey !== null) {
    args.push('--property', 'parse.key=true', '--property', `key.separator=${KEY_SEP}`);
    input = `${effectiveKey}${KEY_SEP}${payload}\n`;
  } else {
    input = payload + '\n';
  }

  const { code, stderr } = await runCommand(
    kafkaBin(cfg, 'kafka-console-producer.sh'),
    args,
    { timeoutMs: 15000, input }
  );

  if (code !== 0) {
    return res.status(500).json({ error: stderr || 'Publish failed' });
  }

  res.json({ ok: true, warning, sentKey: hasKey ? key : (effectiveKey || null) });
});

const PORT = process.env.PORT || 4545;
app.listen(PORT, () => {
  console.log(`Kafka Viz Tool running at http://localhost:${PORT}`);
});
