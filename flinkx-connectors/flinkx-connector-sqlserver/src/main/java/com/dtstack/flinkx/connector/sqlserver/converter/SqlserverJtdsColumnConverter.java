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

package com.dtstack.flinkx.connector.sqlserver.converter;

import com.dtstack.flinkx.conf.FieldConf;
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
import com.dtstack.flinkx.element.column.SqlDateColumn;
import com.dtstack.flinkx.element.column.StringColumn;
import com.dtstack.flinkx.element.column.TimeColumn;
import com.dtstack.flinkx.element.column.TimestampColumn;
import com.dtstack.flinkx.util.StringUtil;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import microsoft.sql.DateTimeOffset;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.TimeZone;

/**
 * Company：www.dtstack.com
 *
 * @author shitou
 * @date 2021/8/12 11:24
 */
public class SqlserverJtdsColumnConverter extends JdbcColumnConverter {

    public SqlserverJtdsColumnConverter(RowType rowType, FlinkxCommonConf commonConf) {
        super(rowType, commonConf);
    }

    @Override
    @SuppressWarnings("unchecked")
    public RowData toInternal(ResultSet resultSet) throws Exception {
        List<FieldConf> fieldConfList = commonConf.getColumn();
        ColumnRowData result = new ColumnRowData(fieldConfList.size());
        int converterIndex = 0;
        for (FieldConf fieldConf : fieldConfList) {
            AbstractBaseColumn baseColumn = null;
            if (StringUtils.isBlank(fieldConf.getValue())) {
                // in sqlserver, timestamp type is a binary array of 8 bytes.
                if ("timestamp".equalsIgnoreCase(fieldConf.getType())) {
                    byte[] value = (byte[]) resultSet.getObject(converterIndex + 1);
                    String hexString = StringUtil.bytesToHexString(value);
                    baseColumn = new BigDecimalColumn(Long.parseLong(hexString, 16));
                } else {
                    baseColumn =
                            (AbstractBaseColumn)
                                    toInternalConverters
                                            .get(converterIndex)
                                            .deserialize(converterIndex + 1);
                }
                converterIndex++;
            }
            result.addField(assembleFieldProps(fieldConf, baseColumn));
        }
        return result;
    }

    @Override
    protected IDeserializationConverter<ResultSet, AbstractBaseColumn> createInternalConverter(
            Integer index) {
        LogicalType type = rowType.getTypeAt(index);
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
                return resultSet -> new StringColumn(resultSet.getString(index));
            case DATE:
                return resultSet -> new SqlDateColumn(resultSet.getDate(index));
            case TIME_WITHOUT_TIME_ZONE:
                return resultSet -> new TimeColumn(resultSet.getTime(index));
            case TIMESTAMP_WITH_TIME_ZONE:
                return resultSet -> {
                    Object val = resultSet.getObject(index);
                    String[] timeAndTimeZone = String.valueOf(val).split(" ");
                    if (timeAndTimeZone.length == 2) {
                        Timestamp timestamp = Timestamp.valueOf(String.valueOf(val));
                        long localTime =
                                timestamp.getTime()
                                        + (long) getMillSecondDiffWithTimeZone(timeAndTimeZone[1]);
                        return new TimestampColumn(localTime, 7);
                    } else {
                        return new TimestampColumn(Timestamp.valueOf(timeAndTimeZone[0]), 7);
                    }
                };
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                int precision = ((TimestampType) (type)).getPrecision();
                return resultSet -> new TimestampColumn(resultSet.getTimestamp(index), precision);
            case BINARY:
            case VARBINARY:
                return resultSet -> new BytesColumn(resultSet.getBytes(index));
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
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
            case INTERVAL_YEAR_MONTH:
                return (val, index, statement) ->
                        statement.setInt(index, ((ColumnRowData) val).getField(index).asYearInt());
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
                return (val, index, statement) ->
                        statement.setString(
                                index, ((ColumnRowData) val).getField(index).asString());
            case DATE:
                return (val, index, statement) ->
                        statement.setDate(index, ((ColumnRowData) val).getField(index).asSqlDate());
            case TIME_WITHOUT_TIME_ZONE:
                return (val, index, statement) ->
                        statement.setTime(index, ((ColumnRowData) val).getField(index).asTime());
            case TIMESTAMP_WITH_TIME_ZONE:
                return (val, index, statement) ->
                        statement.setObject(
                                index,
                                DateTimeOffset.valueOf(
                                        ((ColumnRowData) val).getField(index).asTimestamp(),
                                        getMinutesOffset()));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return (val, index, statement) ->
                        statement.setTimestamp(
                                index, ((ColumnRowData) val).getField(index).asTimestamp());

            case BINARY:
            case VARBINARY:
                return (val, index, statement) ->
                        statement.setBytes(index, ((ColumnRowData) val).getField(index).asBytes());
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }

    public int getMinutesOffset() {
        return getMillSecondOffset() / 1000 / 60;
    }

    public int getMillSecondDiffWithTimeZone(String sqlServerTimeZone) {
        long currentTimeMillis = System.currentTimeMillis();
        TimeZone timeZone = TimeZone.getTimeZone("GMT" + sqlServerTimeZone);
        return timeZone.getOffset(currentTimeMillis) - getMillSecondOffset(currentTimeMillis);
    }

    public int getMillSecondOffset(long time) {
        Calendar calendar = new GregorianCalendar();
        return calendar.getTimeZone().getOffset(time);
    }

    public int getMillSecondOffset() {
        Calendar calendar = new GregorianCalendar();
        return calendar.getTimeZone().getOffset(System.currentTimeMillis());
    }
}
