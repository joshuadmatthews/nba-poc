-- NBA state-machine transactional outbox (one table per target topic).
-- The Temporal worker INSERTs here instead of producing to Kafka directly; Debezium (Kafka
-- Connect) CDC-tails these tables and publishes to Kafka exactly-once via the Outbox Event
-- Router SMT: aggregateid -> message key, payload -> message value, kind -> "kind" header,
-- aggregatetype -> target topic.
CREATE TABLE IF NOT EXISTS outbox_member_facts (
  id            uuid         PRIMARY KEY DEFAULT gen_random_uuid(),
  aggregatetype text         NOT NULL,   -- target topic, e.g. 'nba.member.facts'
  aggregateid   text         NOT NULL,   -- kafka message key
  kind          text,                    -- -> kafka header 'kind'
  payload       text         NOT NULL,   -- kafka message value (JSON)
  created_at    timestamptz  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS outbox_activations (
  id            uuid         PRIMARY KEY DEFAULT gen_random_uuid(),
  aggregatetype text         NOT NULL,   -- target topic, e.g. 'nba.activations'
  aggregateid   text         NOT NULL,
  kind          text,
  payload       text         NOT NULL,
  created_at    timestamptz  NOT NULL DEFAULT now()
);

-- action-library outbox: definition upserts/deletes + operator suppressions route to nba.definitions,
-- inbound dispositions to nba.member.facts (all by aggregatetype). payload NULL => Debezium emits a
-- TOMBSTONE (route.tombstone.on.empty.payload), which a def DELETE needs on the compacted definitions
-- topic. action-library creates this in its initSchema; listed here so all three outbox tables (and the
-- connector's table.include.list / nba_outbox_pub publication) stay in one place.
CREATE TABLE IF NOT EXISTS outbox_defs (
  id            uuid         PRIMARY KEY DEFAULT gen_random_uuid(),
  aggregatetype text         NOT NULL,   -- target topic: 'nba.definitions' | 'nba.member.facts'
  aggregateid   text         NOT NULL,   -- kafka key: 'ACTION:{id}' | 'ACTION_SUPPRESS:{target}' | 'OPERATOR:{entity}'
  kind          text,                    -- -> 'kind' header (e.g. 'disposition', 'action-suppress')
  payload       text,                    -- kafka value (JSON); NULL = tombstone (def delete)
  created_at    timestamptz  NOT NULL DEFAULT now()
);
