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

package com.dtstack.flinkx.connector.oracle.converter;

import com.dtstack.flinkx.conf.FlinkxCommonConf;
import com.dtstack.flinkx.connector.jdbc.converter.JdbcColumnConverter;
import com.dtstack.flinkx.connector.jdbc.statement.FieldNamedPreparedStatement;
import com.dtstack.flinkx.converter.IDeserializationConverter;
import com.dtstack.flinkx.converter.ISerializationConverter;
import com.dtstack.flinkx.element.AbstractBaseColumn;
import com.dtstack.flinkx.element.ColumnRowData;
import com.dtstack.flinkx.element.column.BigDecimalColumn;
import com.dtstack.flinkx.element.column.BooleanColumn;
import com.dtstack.flinkx.element.column.BytesColumn;
import com.dtstack.flinkx.element.column.StringColumn;
import com.dtstack.flinkx.element.column.TimestampColumn;

import org.apache.flink.connector.jdbc.utils.JdbcTypeUtil;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;

import oracle.sql.TIMESTAMP;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringReader;
import java.sql.ResultSet;

/**
 * company www.dtstack.com
 *
 * @author jier
 */
public class OracleColumnConverter extends JdbcColumnConverter {

    public OracleColumnConverter(RowType rowType, FlinkxCommonConf commonConf) {
        super(rowType, commonConf);
    }

    @Override
    protected IDeserializationConverter<ResultSet, AbstractBaseColumn> createInternalConverter(
            Integer index) {
        LogicalType type = rowType.getTypeAt(index - 1);
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return resultSet -> new BooleanColumn(resultSet.getBoolean(index));
            case TINYINT:
                return resultSet -> new BigDecimalColumn(resultSet.getByte(index));
            case SMALLINT:
            case INTEGER:
                return resultSet -> new BigDecimalColumn(resultSet.getInt(index));
            case FLOAT:
                return resultSet -> new BigDecimalColumn(resultSet.getFloat(index));
            case DOUBLE:
                return resultSet -> new BigDecimalColumn(resultSet.getDouble(index));
            case BIGINT:
                return resultSet -> new BigDecimalColumn(resultSet.getLong(index));
            case DECIMAL:
                return resultSet -> new BigDecimalColumn(resultSet.getBigDecimal(index));
            case CHAR:
            case VARCHAR:
                if (type instanceof ClobType) {
                    return resultSet -> {
                        oracle.sql.CLOB clob = (oracle.sql.CLOB) resultSet.getObject(index);
                        return new StringColumn(ConvertUtil.convertClob(clob));
                    };
                }
                return resultSet -> new StringColumn(resultSet.getString(index));
            case DATE:
                return resultSet -> new TimestampColumn(resultSet.getTimestamp(index), 0);
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return resultSet ->
                        new TimestampColumn(
                                ((TIMESTAMP) resultSet.getObject(index)).timestampValue());
            case BINARY:
            case VARBINARY:
                return resultSet -> {
                    if (type instanceof BlobType) {
                        oracle.sql.BLOB blob = (oracle.sql.BLOB) resultSet.getObject(index);
                        byte[] bytes = ConvertUtil.toByteArray(blob);
                        return new BytesColumn(bytes);
                    } else {
                        return new BytesColumn(resultSet.getBytes(index));
                    }
                };
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    @Override
    protected ISerializationConverter<FieldNamedPreparedStatement>
            wrapIntoNullableExternalConverter(
                    ISerializationConverter serializationConverter, Integer integer) {
        return (val, index, statement) -> {
            if (((ColumnRowData) val).getField(index) == null) {
                try {
                    final int sqlType =
                            JdbcTypeUtil.typeInformationToSqlType(
                                    TypeConversions.fromDataTypeToLegacyInfo(
                                            TypeConversions.fromLogicalToDataType(
                                                    rowType.getTypeAt(integer))));
                    statement.setNull(index, sqlType);
                } catch (Exception e) {
                    statement.setObject(index, null);
                }
            } else {
                serializationConverter.serialize(val, index, statement);
            }
        };
    }

    @Override
    protected ISerializationConverter<FieldNamedPreparedStatement> createExternalConverter(
            Integer integer) {
        LogicalType type = rowType.getTypeAt(integer);
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return (val, index, statement) ->
                        statement.setBoolean(
                                index, ((ColumnRowData) val).getField(index).asBoolean());
            case TINYINT:
                return (val, index, statement) -> statement.setByte(index, val.getByte(index));
            case SMALLINT:
            case INTEGER:
                return (val, index, statement) ->
                        statement.setInt(index, ((ColumnRowData) val).getField(index).asInt());
            case FLOAT:
                return (val, index, statement) ->
                        statement.setFloat(index, ((ColumnRowData) val).getField(index).asFloat());
            case DOUBLE:
                return (val, index, statement) ->
                        statement.setDouble(
                                index, ((ColumnRowData) val).getField(index).asDouble());

            case BIGINT:
                return (val, index, statement) ->
                        statement.setLong(index, ((ColumnRowData) val).getField(index).asLong());
            case DECIMAL:
                return (val, index, statement) ->
                        statement.setBigDecimal(
                                index, ((ColumnRowData) val).getField(index).asBigDecimal());
            case CHAR:
            case VARCHAR:
                return (val, index, statement) -> {
                    if (type instanceof ClobType) {
                        try (StringReader reader =
                                new StringReader(
                                        ((ColumnRowData) val).getField(index).asString())) {
                            statement.setClob(index, reader);
                        }
                    } else {
                        statement.setString(
                                index, ((ColumnRowData) val).getField(index).asString());
                    }
                };
            case DATE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (val, index, statement) ->
                        statement.setTimestamp(
                                index, ((ColumnRowData) val).getField(index).asTimestamp());

            case BINARY:
            case VARBINARY:
                return (val, index, statement) -> {
                    if (type instanceof BlobType) {
                        try (InputStream is = new ByteArrayInputStream(val.getBinary(index))) {
                            statement.setBlob(index, is);
                        }
                    } else {
                        statement.setBytes(index, val.getBinary(index));
                    }
                };
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }
}
