// DLQ admin for the Command Center — per-consumer dead-letter monitoring + replay/flush.
// Each consistency consumer writes poison records (envelope-wrapped) to its own nba.dlq.{consumer} topic.
// Envelope = {consumer, topic(source), partition, offset, key, value, headers, error, dlqTs}. Replay
// re-produces the EXACT original record to its SOURCE topic (idempotency is the consumer's job — event-time
// LWW / Temporal workflow-id dedup) then truncates the DLQ; flush truncates without replaying.
import kafkajs from 'kafkajs';
import snappy from 'snappyjs';
const { Kafka, logLevel, CompressionCodecs, CompressionTypes } = kafkajs;
CompressionCodecs[CompressionTypes.Snappy] = () => ({
  compress: async (e) => snappy.compress(e.buffer),
  decompress: async (b) => snappy.uncompress(b),
});

const BROKER = process.env.CC_KAFKA_BROKER || 'nba-redpanda:9092';
const PREFIX = 'nba.dlq.';
const kafka = new Kafka({ clientId: 'cc-dlq', brokers: [BROKER], logLevel: logLevel.ERROR, retry: { retries: 8 } });

// in-memory peek of the most recent poison per consumer (best-effort; depth comes from the broker offsets)
const LAST = new Map();   // consumer -> { error, dlqTs, sourceTopic }
let trackerStarted = false;
export async function startDlqTracker(log = console.log) {
  if (trackerStarted) return; trackerStarted = true;
  try {
    const consumer = kafka.consumer({ groupId: `cc-dlq-tracker-${Date.now()}`, allowAutoTopicCreation: false });
    await consumer.connect();
    await consumer.subscribe({ topics: [new RegExp(`^${PREFIX.replace('.', '\\.')}.*`)], fromBeginning: true });
    await consumer.run({ eachMessage: async ({ topic, message }) => {
      try {
        const e = JSON.parse(message.value.toString('utf8'));
        LAST.set(topic.slice(PREFIX.length), { error: e.error || null, dlqTs: Number(e.dlqTs) || null, sourceTopic: e.topic || null });
      } catch { /* skip */ }
    } });
    log('[dlq] tracker tailing ' + PREFIX + '* for last-error peek');
  } catch (e) { log('[dlq] tracker error: ' + e.message); trackerStarted = false; }
}

export function sumOffsets(offsets) {   // [{partition, offset(high), low}] -> {high,low}
  let high = 0, low = 0;
  for (const o of offsets) { high += Number(o.offset); low += Number(o.low); }
  return { high, low, depth: high - low };
}

// Map a DLQ envelope back to the producer payload that re-produces the EXACT original record to its
// SOURCE topic (key preserved -> same partition; value verbatim; headers carried). Returns null for an
// envelope with no source topic (skip). Pure — the replay seam, unit-tested without a broker.
export function replayRecord(e) {
  if (!e || !e.topic) return null;
  return { topic: e.topic, messages: [{ key: e.key ?? null, value: e.value, headers: e.headers || {} }] };
}

export async function dlqStats() {
  const admin = kafka.admin();
  await admin.connect();
  try {
    const topics = (await admin.listTopics()).filter((t) => t.startsWith(PREFIX)).sort();
    const out = [];
    for (const t of topics) {
      const { depth } = sumOffsets(await admin.fetchTopicOffsets(t));
      const consumer = t.slice(PREFIX.length);
      const last = LAST.get(consumer) || {};
      out.push({ consumer, topic: t, depth, lastError: depth > 0 ? (last.error || null) : null,
        lastTs: depth > 0 ? (last.dlqTs || null) : null, sourceTopic: last.sourceTopic || null });
    }
    return out;
  } finally { await admin.disconnect(); }
}

// Bounded drain of one DLQ topic up to its current high watermark. Calls onMessage(envelope) per record.
async function drainTo(topic, onMessage) {
  const admin = kafka.admin();
  await admin.connect();
  const offsets = await admin.fetchTopicOffsets(topic);
  const { depth } = sumOffsets(offsets);
  await admin.disconnect();
  if (depth === 0) return { count: 0, offsets };
  const highByPart = Object.fromEntries(offsets.map((o) => [o.partition, Number(o.offset)]));
  const consumer = kafka.consumer({ groupId: `cc-dlq-drain-${Date.now()}`, allowAutoTopicCreation: false });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  let count = 0;
  await new Promise(async (resolve) => {
    const reached = {};
    const t = setTimeout(resolve, 15000);   // safety cap
    await consumer.run({ eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning, isStale }) => {
      for (const m of batch.messages) {
        if (!isRunning() || isStale()) break;
        try { await onMessage(JSON.parse(m.value.toString('utf8'))); count++; } catch { /* skip bad envelope */ }
        resolveOffset(m.offset);
      }
      await heartbeat();
      if (batch.messages.length && Number(batch.messages[batch.messages.length - 1].offset) >= (highByPart[batch.partition] ?? 0) - 1) reached[batch.partition] = true;
      if (offsets.every((o) => reached[o.partition] || Number(o.offset) === Number(o.low))) { clearTimeout(t); resolve(); }
    } });
  });
  await consumer.disconnect();
  return { count, offsets, highByPart };
}

async function truncate(topic, offsets, highByPart) {
  const admin = kafka.admin();
  await admin.connect();
  try {
    await admin.fetchTopicMetadata({ topics: [topic] });   // load partition leaders before deleteTopicRecords
    await admin.deleteTopicRecords({ topic, partitions: offsets.map((o) => ({ partition: o.partition, offset: String(highByPart[o.partition] ?? o.offset) })) });
  } finally { await admin.disconnect(); }
}

export async function replayDlq(consumerName) {
  const topic = PREFIX + consumerName;
  const producer = kafka.producer();
  await producer.connect();
  let replayed = 0;
  const { count, offsets, highByPart } = await drainTo(topic, async (e) => {
    const msg = replayRecord(e);
    if (!msg) return;
    await producer.send(msg);
    replayed++;
  });
  await producer.disconnect();
  if (count > 0 && highByPart) await truncate(topic, offsets, highByPart);
  LAST.delete(consumerName);
  return { consumer: consumerName, replayed };
}

export async function flushDlq(consumerName) {
  const topic = PREFIX + consumerName;
  const { count, offsets, highByPart } = await drainTo(topic, async () => {});   // count + watermarks only
  if (count > 0 && highByPart) await truncate(topic, offsets, highByPart);
  LAST.delete(consumerName);
  return { consumer: consumerName, flushed: count };
}
