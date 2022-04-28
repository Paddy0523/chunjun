CREATE TABLE source
(
    rowkey VARCHAR,
    info ROW<_int_ int,_string_ varchar>,
    detail ROW<_double_ double,_long_ BIGINT,_boolean_ BOOLEAN,_bigdecimal_ decimal,_short_ SMALLINT,_float_ float>,
    PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
    'connector' = 'hbase2-x'
    ,'zookeeper.quorum' = 'dwhdp001,dwhdp002,dwhdp003'
      ,'zookeeper.znode.parent' = '/hbase-unsecure'
    ,'null-string-literal' = 'null'
    ,'sink.buffer-flush.max-size' = '1000'
    ,'sink.buffer-flush.max-rows' = '1000'
    ,'sink.buffer-flush.interval' = '60'
    ,'table-name' = 'test_sink'
);

CREATE TABLE sink
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
    'connector' = 'stream-x'
);

insert into sink
SELECT rowkey,info._int_,info._string_,detail._double_,detail._long_,detail._boolean_,detail._bigdecimal_,detail._short_,detail._float_
from source u;
