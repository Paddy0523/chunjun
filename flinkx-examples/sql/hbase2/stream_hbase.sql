CREATE TABLE source
(
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
    'connector' = 'stream-x',
    'number-of-rows' = '5'
);

CREATE TABLE sink
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
    ,'table-name' = 'test_source'
);

insert into sink
SELECT rowkey,ROW(_int_, _string_) as info,ROW(_double_, _long_, _boolean_, _bigdecimal_, _short_, _float_) as detail
from source u;
