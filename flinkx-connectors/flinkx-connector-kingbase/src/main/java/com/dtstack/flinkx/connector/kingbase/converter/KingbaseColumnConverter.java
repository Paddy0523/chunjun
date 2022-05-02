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
package com.dtstack.flinkx.connector.kingbase.converter;

import com.dtstack.flinkx.conf.FlinkxCommonConf;
import com.dtstack.flinkx.connector.jdbc.converter.JdbcColumnConverter;
import com.dtstack.flinkx.converter.IDeserializationConverter;
import com.dtstack.flinkx.element.AbstractBaseColumn;
import com.dtstack.flinkx.element.column.BigDecimalColumn;
import com.dtstack.flinkx.element.column.BooleanColumn;
import com.dtstack.flinkx.element.column.BytesColumn;
import com.dtstack.flinkx.element.column.SqlDateColumn;
import com.dtstack.flinkx.element.column.StringColumn;
import com.dtstack.flinkx.element.column.TimeColumn;
import com.dtstack.flinkx.element.column.TimestampColumn;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.sql.ResultSet;

/**
 * @description:
 * @program: flinkx-all
 * @author: lany
 * @create: 2021/05/19 15:26
 */
public class KingbaseColumnConverter extends JdbcColumnConverter {

    public KingbaseColumnConverter(RowType rowType, FlinkxCommonConf commonConf) {
        super(rowType, commonConf);
    }

    /**
     * override reason: tinying type in KingBase is byte type, couldn't case int.
     *
     * @param index
     * @return
     */
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
                return resultSet -> new StringColumn(resultSet.getString(index));
            case DATE:
                return resultSet -> new SqlDateColumn(resultSet.getDate(index));
            case TIME_WITHOUT_TIME_ZONE:
                return resultSet -> new TimeColumn(resultSet.getTime(index));
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return resultSet -> new TimestampColumn(resultSet.getTimestamp(index));
            case BINARY:
            case VARBINARY:
                return resultSet -> new BytesColumn(resultSet.getBytes(index));
            default:
                throw new UnsupportedOperationException("Unsupported type:" + type);
        }
    }
}
