SET 'auto.offset.reset'='earliest';

CREATE STREAM IF NOT EXISTS raw_events_stream (
  id_evenement VARCHAR,
  horodatage VARCHAR,
  identifiant_utilisateur VARCHAR,
  adresse_ip VARCHAR,
  pays VARCHAR,
  type_appareil VARCHAR,
  systeme_exploitation VARCHAR,
  type_evenement VARCHAR,
  id_session VARCHAR,
  latitude DOUBLE,
  longitude DOUBLE,
  latence_requete_ms INT,
  methode_authentification VARCHAR,
  age_compte_jours INT,
  utilise_vpn BOOLEAN,
  empreinte_appareil VARCHAR,
  evt_id VARCHAR,
  ts VARCHAR,
  uid VARCHAR,
  source_ip VARCHAR,
  region VARCHAR,
  device_class VARCHAR,
  platform VARCHAR,
  action VARCHAR,
  sess_id VARCHAR,
  lat DOUBLE,
  lng DOUBLE,
  delay_ms INT,
  auth_type VARCHAR,
  account_days INT,
  via_vpn BOOLEAN,
  device_hash VARCHAR
) WITH (
  KAFKA_TOPIC='data.stream.raw',
  VALUE_FORMAT='JSON'
);

CREATE OR REPLACE STREAM producer1_events AS
SELECT
  id_evenement AS event_id,
  horodatage AS event_timestamp_raw,
  PARSE_TIMESTAMP(horodatage, 'yyyy-MM-dd''T''HH:mm:ssX') AS event_timestamp,
  identifiant_utilisateur AS user_id,
  adresse_ip AS ip_address,
  pays AS country_code,
  type_appareil AS device_type,
  systeme_exploitation AS operating_system,
  type_evenement AS event_type,
  id_session AS session_id,
  latitude,
  longitude,
  latence_requete_ms AS request_latency_ms,
  methode_authentification AS auth_method,
  age_compte_jours AS account_age_days,
  utilise_vpn AS uses_vpn,
  empreinte_appareil AS device_fingerprint,
  'producer1' AS source_producer,
  ROWTIME AS normalization_timestamp
FROM raw_events_stream
WHERE id_evenement IS NOT NULL
EMIT CHANGES;

CREATE OR REPLACE STREAM producer2_events AS
SELECT
  evt_id AS event_id,
  ts AS event_timestamp_raw,
  PARSE_TIMESTAMP(ts, 'yyyy-MM-dd''T''HH:mm:ssX') AS event_timestamp,
  uid AS user_id,
  source_ip AS ip_address,
  region AS country_code,
  device_class AS device_type,
  platform AS operating_system,
  action AS event_type,
  sess_id AS session_id,
  lat AS latitude,
  lng AS longitude,
  delay_ms AS request_latency_ms,
  auth_type AS auth_method,
  account_days AS account_age_days,
  via_vpn AS uses_vpn,
  device_hash AS device_fingerprint,
  'producer2' AS source_producer,
  ROWTIME AS normalization_timestamp
FROM raw_events_stream
WHERE evt_id IS NOT NULL
EMIT CHANGES;

CREATE OR REPLACE STREAM normalized_events_stream WITH (
  KAFKA_TOPIC='data.stream.normalized',
  VALUE_FORMAT='AVRO',
  VALUE_AVRO_SCHEMA_FULL_NAME='com.datastreaming.events.NormalizedEvent'
) AS
SELECT *
FROM producer1_events
EMIT CHANGES;

INSERT INTO normalized_events_stream
SELECT *
FROM producer2_events
EMIT CHANGES;
