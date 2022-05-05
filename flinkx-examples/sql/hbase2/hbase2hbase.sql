CREATE TABLE hbase_source
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
      ,'table-name' = 'test_source'
      );



CREATE TABLE hbase_sink
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
      ,'sink.buffer-flush.max-rows' = '1'
      ,'sink.buffer-flush.interval' = '60'
      ,'table-name' = 'test_sink'
      );


INSERT INTO hbase_sink
SELECT rowkey,ROW(_int_, _string_) as info,ROW(_double_, _long_, _boolean_, _bigdecimal_, _short_, _float_) as detail
FROM hbase_source;
