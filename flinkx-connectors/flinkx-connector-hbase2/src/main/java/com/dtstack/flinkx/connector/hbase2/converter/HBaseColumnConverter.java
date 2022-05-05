/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.connector.hbase2.converter;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.hbase2.common.HBaseTableSchema;
import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.converter.IDeserializationConverter;
import com.dtstack.flinkx.converter.ISerializationConverter;
import com.dtstack.flinkx.element.AbstractBaseColumn;
import com.dtstack.flinkx.element.column.*;
import com.dtstack.flinkx.throwable.FlinkxRuntimeException;
import com.dtstack.flinkx.throwable.UnsupportedTypeException;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * @author wuren
 * @program flinkx
 * @create 2021/04/30
 */
public class HBaseColumnConverter
        extends AbstractRowConverter<Result, RowData, Object, LogicalType> {

    private final HBaseTableSchema hBaseTableSchema;
    private Map<String, Integer> indexes = new HashMap<>();

    private final int rowKeyIndex;

    public HBaseColumnConverter(List<FieldConf> fieldConfList, HBaseTableSchema hBaseTableSchema) {
        super(fieldConfList.size());
        this.hBaseTableSchema = hBaseTableSchema;
        this.buildIndexes(fieldConfList);
        this.rowKeyIndex = getRowKeyIndex();

        if (rowKeyIndex >= 0) {
            Optional<DataType> rowKeyType = hBaseTableSchema.getRowKeyDataType();
            if (rowKeyType.isPresent()) {
                DataType dataType = rowKeyType.get();
                LogicalType logicalType = dataType.getLogicalType();
                toInternalConverters.add(
                        rowKeyIndex,
                        wrapIntoNullableInternalConverter(createInternalConverter(logicalType)));
            } else {
                throw new RuntimeException("Rowkey type must not be null");
            }
        }

        String[] familyNames = hBaseTableSchema.getFamilyNames();
        for (String familyName : familyNames) {
            Map<String, DataType> qualifier = hBaseTableSchema.getFamilyInfo(familyName);
            for (Map.Entry<String, DataType> qualifierEntry : qualifier.entrySet()) {
                String qualifierName = qualifierEntry.getKey();
                DataType qualifierType = qualifierEntry.getValue();
                int index = getQualifierIndex(familyName, qualifierName);
                toInternalConverters.add(
                        index,
                        wrapIntoNullableInternalConverter(
                                createInternalConverter(qualifierType.getLogicalType())));
                //                toExternalConverters.add(index,
                // wrapIntoNullableExternalConverter(createExternalConverter(qualifierType.getLogicalType()), type));
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public RowData toInternal(Result result) throws Exception {
        GenericRowData rowData = new GenericRowData(indexes.size());
        if (rowKeyIndex >= 0) {
            byte[] cell = result.getRow();
            AbstractBaseColumn rowKeyColumn =
                    (AbstractBaseColumn) toInternalConverters.get(rowKeyIndex).deserialize(cell);
            rowData.setField(rowKeyIndex, rowKeyColumn);
        }
        String[] familyNames = hBaseTableSchema.getFamilyNames();
        for (String familyName : familyNames) {
            byte[] familyBytes = Bytes.toBytes(familyName);
            Map<String, DataType> qualifier = hBaseTableSchema.getFamilyInfo(familyName);
            for (Map.Entry<String, DataType> qualifierEntry : qualifier.entrySet()) {
                String qualifierName = qualifierEntry.getKey();
                byte[] qualifierBytes = Bytes.toBytes(qualifierName);
                Cell cell = result.getColumnLatestCell(familyBytes, qualifierBytes);
                if (cell != null) {
                    int offset = cell.getValueOffset();
                    int length = cell.getValueLength();
                    byte[] data = cell.getValueArray();
                    byte[] value = new byte[length];
                    System.arraycopy(data, offset, value, 0, length);
                    int index = getQualifierIndex(familyName, qualifierName);
                    AbstractBaseColumn baseColumn =
                            (AbstractBaseColumn) toInternalConverters.get(index).deserialize(value);
                    rowData.setField(index, baseColumn);
                }
            }
        }
        return rowData;
    }

    private int getRowKeyIndex() {
        Integer index = indexes.get("rowkey".toUpperCase(Locale.ROOT));
        return index != null ? index : -1;
    }

    private int getQualifierIndex(String familyName, String qualifierName) {
        String key = familyName + ":" + qualifierName;
        return indexes.get(key);
    }

    @SuppressWarnings("unchecked")
    public Object[] toExternal(RowData rowData, Object[] data) throws Exception {
        for (int index = 0; index < rowData.getArity(); index++) {
            toExternalConverters.get(index).serialize(rowData, index, data);
        }
        return data;
    }

    @Override
    public RowData toInternalLookup(RowData input) {
        throw new FlinkxRuntimeException("HDFS Connector doesn't support Lookup Table Function.");
    }

    @Override
    public Object toExternal(RowData rowData, Object output) throws Exception {
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected ISerializationConverter<Object[]> wrapIntoNullableExternalConverter(
            ISerializationConverter serializationConverter, LogicalType type) {
        return (rowData, index, data) -> {
            if (rowData == null || rowData.isNullAt(index)) {
                data[index] = null;
            } else {
                serializationConverter.serialize(rowData, index, data);
            }
        };
    }

    @Override
    @SuppressWarnings("all")
    protected IDeserializationConverter createInternalConverter(LogicalType logicalType) {
        switch (logicalType.getTypeRoot()) {
            case BOOLEAN:
                return new IDeserializationConverter<byte[], AbstractBaseColumn>() {
                    @Override
                    public AbstractBaseColumn deserialize(byte[] bytes) throws Exception {
                        Boolean result = Bytes.toBoolean(bytes);
                        return new BooleanColumn(result);
                    }
                };
            case BIGINT:
                return new IDeserializationConverter<byte[], AbstractBaseColumn>() {
                    @Override
                    public AbstractBaseColumn deserialize(byte[] bytes) throws Exception {
                        Long value = Bytes.toLong(bytes);
                        return new BigDecimalColumn(value);
                    }
                };
            case SMALLINT:
                return new IDeserializationConverter<byte[], AbstractBaseColumn>() {
                    @Override
                    public AbstractBaseColumn deserialize(byte[] bytes) throws Exception {
                        Short value = Bytes.toShort(bytes);
                        return new BigDecimalColumn(value);
                    }
                };
            case DOUBLE:
                return new IDeserializationConverter<byte[], AbstractBaseColumn>() {
                    @Override
                    public AbstractBaseColumn deserialize(byte[] bytes) throws Exception {
                        Double value = Bytes.toDouble(bytes);
                        return new BigDecimalColumn(value);
                    }
                };
            case FLOAT:
                return new IDeserializationConverter<byte[], AbstractBaseColumn>() {
                    @Override
                    public AbstractBaseColumn deserialize(byte[] bytes) throws Exception {
                        Float value = Bytes.toFloat(bytes);
                        return new BigDecimalColumn(value);
                    }
                };
            case DECIMAL:
                return new IDeserializationConverter<byte[], AbstractBaseColumn>() {
                    @Override
                    public AbstractBaseColumn deserialize(byte[] bytes) throws Exception {
                        BigDecimal value = Bytes.toBigDecimal(bytes);
                        return new BigDecimalColumn(value);
                    }
                };
            case INTEGER:
                return new IDeserializationConverter<byte[], AbstractBaseColumn>() {
                    @Override
                    public AbstractBaseColumn deserialize(byte[] bytes) throws Exception {
                        Integer value = Bytes.toInt(bytes);
                        return new BigDecimalColumn(value);
                    }
                };
            case VARCHAR:
                return new IDeserializationConverter<byte[], AbstractBaseColumn>() {
                    @Override
                    public AbstractBaseColumn deserialize(byte[] bytes) throws Exception {
                        String value = Bytes.toString(bytes);
                        return new StringColumn(value);
                    }
                };
            default:
                throw new UnsupportedTypeException(logicalType.getTypeRoot());
        }
    }

    @Override
    protected ISerializationConverter<Object[]> createExternalConverter(LogicalType logicalType) {
        return null;
        /*switch (type.toUpperCase(Locale.ENGLISH)) {
            case "BOOLEAN":
                return (rowData, index, data) -> data[index] = rowData.getBoolean(index);
            case "TINYINT":
                return (rowData, index, data) -> data[index] = rowData.getByte(index);
            case "SMALLINT":
                return (rowData, index, data) -> data[index] = rowData.getShort(index);
            case "INT":
            case "INTEGER":
                return (rowData, index, data) -> data[index] = rowData.getInt(index);
            case "LONG":
            case "BIGINT":
                return (rowData, index, data) -> data[index] = rowData.getLong(index);
            case "FLOAT":
                return (rowData, index, data) -> data[index] = rowData.getFloat(index);
            case "DOUBLE":
                return (rowData, index, data) -> data[index] = rowData.getDouble(index);
            case "DECIMAL":
                //                return (rowData, index, data) -> {
                //                    ColumnTypeUtil.DecimalInfo decimalInfo =
                // decimalColInfo.get(ColumnNameList.get(index));
                //                    HiveDecimal hiveDecimal = HiveDecimal.create(new
                // BigDecimal(rowData.getString(index).toString()));
                //                    hiveDecimal = HiveDecimal.enforcePrecisionScale(hiveDecimal,
                // decimalInfo.getPrecision(), decimalInfo.getScale());
                //                    if(hiveDecimal == null){
                //                        String msg = String.format("The [%s] data data [%s]
                // precision and scale do not match the metadata:decimal(%s, %s)", index,
                // decimalInfo.getPrecision(), decimalInfo.getScale(), rowData);
                //                        throw new WriteRecordException(msg, new
                // IllegalArgumentException());
                //                    }
                //                    data[index] = new HiveDecimalWritable(hiveDecimal);
                //                };
            case "STRING":
            case "VARCHAR":
            case "CHAR":
                return (rowData, index, data) -> data[index] = rowData.getString(index).toString();
            case "TIMESTAMP":
                return (rowData, index, data) ->
                        data[index] = rowData.getTimestamp(index, 6).toTimestamp();
            case "DATE":
                return (rowData, index, data) ->
                        data[index] = new Date(rowData.getTimestamp(index, 6).getMillisecond());
            case "BINARY":
            case "VARBINARY":
                return (rowData, index, data) ->
                        data[index] = new BytesWritable(rowData.getBinary(index));
            case "TIME_WITHOUT_TIME_ZONE":
                //                final int timePrecision = getPrecision(type);
                //                if (timePrecision < MIN_TIME_PRECISION || timePrecision >
                // MAX_TIME_PRECISION) {
                //                    throw new UnsupportedOperationException(
                //                            String.format(
                //                                    "The precision %s of TIME type is out of the
                // range [%s, %s] supported by "
                //                                            + "HBase connector",
                //                                    timePrecision, MIN_TIME_PRECISION,
                // MAX_TIME_PRECISION));
                //                }
            case "TIMESTAMP_WITHOUT_TIME_ZONE":
            case "TIMESTAMP_WITH_LOCAL_TIME_ZONE":
                //                final int timestampPrecision = getPrecision(type);
                //                if (timestampPrecision < MIN_TIMESTAMP_PRECISION
                //                        || timestampPrecision > MAX_TIMESTAMP_PRECISION) {
                //                    throw new UnsupportedOperationException(
                //                            String.format(
                //                                    "The precision %s of TIMESTAMP type is out of
                // the range [%s, %s] supported by "
                //                                            + "HBase connector",
                //                                    timestampPrecision,
                //                                    MIN_TIMESTAMP_PRECISION,
                //                                    MAX_TIMESTAMP_PRECISION));
                //                }
            case "TIMESTAMP_WITH_TIME_ZONE":
            case "INTERVAL_YEAR_MONTH":
            case "INTERVAL_DAY_TIME":
            case "ARRAY":
            case "MULTISET":
            case "MAP":
            case "ROW":
            case "STRUCTURED_TYPE":
            case "DISTINCT_TYPE":
            case "RAW":
            case "NULL":
            case "SYMBOL":
            case "UNRESOLVED":
            default:
                throw new UnsupportedTypeException(type);
        }*/
    }

    void buildIndexes(List<FieldConf> fieldConfList) {
        for (int i = 0; i < fieldConfList.size(); i++) {
            FieldConf fieldConf = fieldConfList.get(i);
            String fieldName = fieldConf.getName();
            if ("rowkey".equalsIgnoreCase(fieldName)) {
                indexes.put(fieldName.toUpperCase(Locale.ROOT), i);
            } else {
                indexes.put(fieldConf.getName(), i);
            }
        }
    }
}
