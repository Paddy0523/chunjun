CREATE TABLE k_source
(
    id        varchar,
    PROCTIME AS PROCTIME()
) WITH (
      'connector' = 'kafka-x'
      ,'topic' = 'jier_test_for_hbase'
      ,'properties.bootstrap.servers' = '172.16.100.109:9092'
      ,'properties.group.id' = 'mowen_g'
      ,'scan.startup.mode' = 'latest-offset'
      ,'format' = 'json'
      ,'json.timestamp-format.standard' = 'SQL'
      );


CREATE TABLE hbase_lookup
(
    rowkey VARCHAR,
    info ROW<_int_ int, _string_ varchar>,
    detail ROW<_double_ double, _long_ BIGINT, _boolean_ boolean, _bigdecimal_ decimal, _short_ SMALLINT, _float_ float>,
    PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
    'connector' = 'hbase2-x'
    ,'zookeeper.quorum' = 'dwhdp001,dwhdp002,dwhdp003'
    ,'zookeeper.znode.parent' = '/hbase-unsecure'
    ,'null-string-literal' = 'null'
    ,'sink.buffer-flush.max-size' = '1000'
    ,'sink.buffer-flush.max-rows' = '1000'
    ,'sink.buffer-flush.interval' = '60'
    ,'lookup.cache-type' = 'lru'
      ,'table-name' = 'test_sink'
);


CREATE TABLE sink
(
    id VARCHAR,
    rowkey VARCHAR,
    _int_ int,
    _string_ VARCHAR,
    _double_ double,
    _long_ BIGINT,
    _boolean_ BOOLEAN,
    _bigdecimal_ decimal,
    _short_ SMALLINT,
    _float_ float
) WITH (
      'connector' = 'stream-x'
      );

INSERT INTO
sink
SELECT
    a.id,
    b.rowkey,
    b.info._int_,
    b.info._string_,
    b.detail._double_,
    b.detail._long_,
    b.detail._boolean_,
    b.detail._bigdecimal_,
    b.detail._short_,
    b.detail._float_
FROM k_source a
LEFT JOIN hbase_lookup FOR SYSTEM_TIME AS OF a.PROCTIME AS b ON a.id = b.rowkey;
