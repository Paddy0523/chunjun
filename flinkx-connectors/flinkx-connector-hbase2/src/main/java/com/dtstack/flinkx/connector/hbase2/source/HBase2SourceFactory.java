/*
 *    Copyright 2021 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package com.dtstack.flinkx.connector.hbase2.source;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.conf.SyncConf;
import com.dtstack.flinkx.connector.hbase2.common.HBaseTableSchema;
import com.dtstack.flinkx.connector.hbase2.conf.HBaseConf;
import com.dtstack.flinkx.connector.hbase2.converter.HBaseColumnConverter;
import com.dtstack.flinkx.connector.hbase2.converter.HBaseRawTypeConverter;
import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.converter.RawTypeConverter;
import com.dtstack.flinkx.source.SourceFactory;
import com.dtstack.flinkx.util.GsonUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class HBase2SourceFactory extends SourceFactory {

    private static final Logger LOG = LoggerFactory.getLogger(HBase2SourceFactory.class);

    private final HBaseConf config;

    public HBase2SourceFactory(SyncConf syncConf, StreamExecutionEnvironment env) {
        super(syncConf, env);
        config =
                GsonUtil.GSON.fromJson(
                        GsonUtil.GSON.toJson(syncConf.getReader().getParameter()), HBaseConf.class);
        super.initFlinkxCommonConf(config);
        config.setTable(syncConf.getReader().getTable().getTableName());
        config.setColumnMetaInfos(syncConf.getReader().getFieldList());
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return new HBaseRawTypeConverter();
    }

    @Override
    @SuppressWarnings("all")
    public DataStream<RowData> createSource() {
        //        throw new RuntimeException("unsupport");
        List<FieldConf> fieldConfList = config.getColumnMetaInfos();
        HBaseTableSchema hBaseTableSchema = buildHBaseTableSchema(config.getTable(), fieldConfList);
        HBaseInputFormatBuilder builder = HBaseInputFormatBuilder.newBuild(hBaseTableSchema);

        builder.setColumnMetaInfos(config.getColumnMetaInfos());
        builder.setConfig(config);
        builder.setColumnMetaInfos(config.getColumnMetaInfos());
        builder.setEncoding(config.getEncoding());
        builder.setHbaseConfig(config.getHbaseConfig());
        builder.setEndRowKey(config.getEndRowkey());
        builder.setIsBinaryRowkey(config.isBinaryRowkey());
        builder.setScanCacheSize(config.getScanCacheSize());
        builder.setStartRowKey(config.getStartRowkey());
        AbstractRowConverter rowConverter =
                new HBaseColumnConverter(fieldConfList, hBaseTableSchema);
        builder.setRowConverter(rowConverter);
        return createInput(builder.finish());
    }

    HBaseTableSchema buildHBaseTableSchema(String tableName, List<FieldConf> fieldConfList) {
        HBaseTableSchema hbaseSchema = new HBaseTableSchema();
        hbaseSchema.setTableName(tableName);
        RawTypeConverter rawTypeConverter = getRawTypeConverter();
        for (FieldConf fieldConf : fieldConfList) {
            String fieldName = fieldConf.getName();
            DataType dataType = rawTypeConverter.apply(fieldConf.getType());
            if ("rowkey".equalsIgnoreCase(fieldName)) {
                hbaseSchema.setRowKey(fieldName, dataType);
            } else if (fieldName.contains(":")) {
                String[] fields = fieldName.split(":");
                hbaseSchema.addColumn(fields[0], fields[1], dataType);
            }
        }
        return hbaseSchema;
    }
}
