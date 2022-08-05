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

package com.dtstack.chunjun.connector.hbase.converter;

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.connector.hbase.conf.HBaseConf;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.element.column.BigDecimalColumn;
import com.dtstack.chunjun.element.column.BooleanColumn;
import com.dtstack.chunjun.element.column.ByteColumn;
import com.dtstack.chunjun.element.column.BytesColumn;
import com.dtstack.chunjun.element.column.DoubleColumn;
import com.dtstack.chunjun.element.column.FloatColumn;
import com.dtstack.chunjun.element.column.IntColumn;
import com.dtstack.chunjun.element.column.LongColumn;
import com.dtstack.chunjun.element.column.ShortColumn;
import com.dtstack.chunjun.element.column.SqlDateColumn;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.element.column.TimeColumn;
import com.dtstack.chunjun.element.column.TimestampColumn;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.client.Mutation;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

public class HBaseColumnConverterTest {

    /**
     * The number of milliseconds in a day.
     *
     * <p>This is the modulo 'mask' used when converting TIMESTAMP values to DATE and TIME values.
     */
    private static final long MILLIS_PER_DAY = 86400000L; // = 24 * 60 * 60 * 1000

    /** The local time zone. */
    private static final TimeZone LOCAL_TZ = TimeZone.getDefault();

    @Before
    public void setUp() {}

    @Test
    public void testConstructorOfHBaseColumnConverter() throws Exception {
        Map<String, Object> confMap = Maps.newHashMap();
        List<FieldConf> columnList = Lists.newArrayList();
        HBaseConf conf = new HBaseConf();
        ColumnRowData rowData = new ColumnRowData(RowKind.INSERT, 14);

        FieldConf id = new FieldConf();
        id.setName("stu:id");
        id.setType("int");
        rowData.addField(new IntColumn(1));

        FieldConf decimal_val = new FieldConf();
        decimal_val.setName("msg:decimal_val");
        decimal_val.setType("decimal(38, 18)");
        rowData.addField(new BigDecimalColumn(new BigDecimal("3.3")));

        FieldConf float_val = new FieldConf();
        float_val.setName("msg:float_val");
        float_val.setType("float");
        rowData.addField(new FloatColumn(3.33f));

        FieldConf smallint_val = new FieldConf();
        smallint_val.setName("msg:smallint_val");
        smallint_val.setType("smallint");
        rowData.addField(new ShortColumn((short) 3));

        FieldConf bigint_val = new FieldConf();
        bigint_val.setName("msg:bigint_val");
        bigint_val.setType("bigint");
        rowData.addField(new LongColumn(1));

        FieldConf boolean_val = new FieldConf();
        boolean_val.setName("msg:boolean_val");
        boolean_val.setType("boolean");
        rowData.addField(new BooleanColumn(false));

        FieldConf tinyint_val = new FieldConf();
        tinyint_val.setName("msg:tinyint_val");
        tinyint_val.setType("tinyint");
        rowData.addField(new ByteColumn((byte) 1));

        FieldConf date_val = new FieldConf();
        date_val.setName("msg:date_val");
        date_val.setType("date");
        rowData.addField(new SqlDateColumn(Date.valueOf("2022-08-26")));

        FieldConf time_val = new FieldConf();
        time_val.setName("msg:time_val");
        time_val.setType("time");
        rowData.addField(new TimeColumn(Time.valueOf("11:06:14")));

        FieldConf timestamp_val = new FieldConf();
        timestamp_val.setName("msg:timestamp_val");
        timestamp_val.setType("timestamp(3)");
        rowData.addField(new TimestampColumn(System.currentTimeMillis()));

        FieldConf datetime_val = new FieldConf();
        datetime_val.setName("msg:datetime_val");
        datetime_val.setType("datetime");
        rowData.addField(new TimestampColumn(System.currentTimeMillis()));

        FieldConf bytes_val = new FieldConf();
        bytes_val.setName("msg:bytes_val");
        bytes_val.setType("bytes");
        rowData.addField(new BytesColumn("test".getBytes(StandardCharsets.UTF_8)));

        FieldConf varchar_val = new FieldConf();
        varchar_val.setName("msg:varchar_val");
        varchar_val.setType("varchar(255)");
        rowData.addField(new StringColumn("test"));

        FieldConf double_val = new FieldConf();
        double_val.setName("msg:double_val");
        double_val.setType("double");
        rowData.addField(new DoubleColumn(3.33d));

        FieldConf val_1 = new FieldConf();
        val_1.setName("val_1");
        val_1.setType("string");
        val_1.setValue("val_1");

        columnList.add(id);
        columnList.add(decimal_val);
        columnList.add(float_val);
        columnList.add(smallint_val);
        columnList.add(bigint_val);
        columnList.add(boolean_val);
        columnList.add(tinyint_val);
        columnList.add(date_val);
        columnList.add(time_val);
        columnList.add(timestamp_val);
        columnList.add(datetime_val);
        columnList.add(bytes_val);
        columnList.add(varchar_val);
        columnList.add(double_val);
        columnList.add(val_1);

        conf.setHbaseConfig(confMap);
        conf.setColumn(columnList);
        conf.setEncoding(StandardCharsets.UTF_8.name());
        conf.setStartRowkey("start");
        conf.setEndRowkey("end");
        conf.setBinaryRowkey(true);
        conf.setTable("test_table");
        conf.setScanCacheSize(1000);

        conf.setNullMode("TEST_NULL_MODE");
        conf.setNullStringLiteral("N/A");
        conf.setWalFlag(true);
        conf.setWriteBufferSize(1000);
        conf.setRowkeyExpress("$(stu:id)");
        conf.setVersionColumnIndex(1);
        conf.setVersionColumnValue("VERSION");

        RowType rowType = TableUtil.createRowType(conf.getColumn(), HBaseRawTypeConverter.INSTANCE);
        HBaseColumnConverter converter = new HBaseColumnConverter(conf, rowType);

        Assert.assertEquals("stu:id", converter.getCommonConf().getColumn().get(0).getName());

        Mutation toExternal = converter.toExternal(rowData, null);
        Assert.assertFalse(toExternal.getFamilyCellMap().isEmpty());
    }

    @Test
    public void testConstructorOfHBaseFlatRowConverter() throws Exception {
        Map<String, Object> confMap = Maps.newHashMap();
        List<FieldConf> columnList = Lists.newArrayList();
        HBaseConf conf = new HBaseConf();
        GenericRowData rowData = new GenericRowData(RowKind.INSERT, 12);

        FieldConf id = new FieldConf();
        id.setName("stu:id");
        id.setType("int");
        rowData.setField(0, 1);

        FieldConf decimal_val = new FieldConf();
        decimal_val.setName("msg:decimal_val");
        decimal_val.setType("decimal(38, 18)");
        rowData.setField(1, DecimalData.fromBigDecimal(new BigDecimal("3.3"), 38, 18));

        FieldConf float_val = new FieldConf();
        float_val.setName("msg:float_val");
        float_val.setType("float");
        rowData.setField(2, 3.3f);

        FieldConf bigint_val = new FieldConf();
        bigint_val.setName("msg:bigint_val");
        bigint_val.setType("bigint");
        rowData.setField(3, 1L);

        FieldConf boolean_val = new FieldConf();
        boolean_val.setName("msg:boolean_val");
        boolean_val.setType("boolean");
        rowData.setField(4, false);

        FieldConf date_val = new FieldConf();
        date_val.setName("msg:date_val");
        date_val.setType("date");
        rowData.setField(5, HBaseColumnConverterTest.dateToInternal(Date.valueOf("2022-08-26")));

        FieldConf time_val = new FieldConf();
        time_val.setName("msg:time_val");
        time_val.setType("time");
        rowData.setField(6, HBaseColumnConverterTest.timeToInternal(Time.valueOf("11:06:14")));

        FieldConf timestamp_val = new FieldConf();
        timestamp_val.setName("msg:timestamp_val");
        timestamp_val.setType("timestamp(3)");
        rowData.setField(7, TimestampData.fromTimestamp(Timestamp.valueOf("2022-08-24 11:08:09")));

        FieldConf datetime_val = new FieldConf();
        datetime_val.setName("msg:datetime_val");
        datetime_val.setType("datetime");
        rowData.setField(8, TimestampData.fromTimestamp(Timestamp.valueOf("2022-08-24 11:08:09")));

        FieldConf bytes_val = new FieldConf();
        bytes_val.setName("msg:bytes_val");
        bytes_val.setType("bytes");
        rowData.setField(9, "test".getBytes(StandardCharsets.UTF_8));

        FieldConf varchar_val = new FieldConf();
        varchar_val.setName("msg:varchar_val");
        varchar_val.setType("varchar(255)");
        rowData.setField(10, StringData.fromString("test"));

        FieldConf double_val = new FieldConf();
        double_val.setName("msg:double_val");
        double_val.setType("double");
        rowData.setField(11, 3.3);

        columnList.add(id);
        columnList.add(decimal_val);
        columnList.add(float_val);
        columnList.add(bigint_val);
        columnList.add(boolean_val);
        columnList.add(date_val);
        columnList.add(time_val);
        columnList.add(timestamp_val);
        columnList.add(datetime_val);
        columnList.add(bytes_val);
        columnList.add(varchar_val);
        columnList.add(double_val);

        conf.setHbaseConfig(confMap);
        conf.setColumn(columnList);
        conf.setEncoding(StandardCharsets.UTF_8.name());
        conf.setStartRowkey("start");
        conf.setEndRowkey("end");
        conf.setBinaryRowkey(true);
        conf.setTable("test_table");
        conf.setScanCacheSize(1000);

        conf.setNullMode("TEST_NULL_MODE");
        conf.setNullStringLiteral("N/A");
        conf.setWalFlag(true);
        conf.setWriteBufferSize(1000);
        conf.setRowkeyExpress("$(stu:id)");
        conf.setVersionColumnIndex(null);
        conf.setVersionColumnValue("2022-08-24 11:08:09");

        RowType rowType = TableUtil.createRowType(conf.getColumn(), HBaseRawTypeConverter.INSTANCE);
        HBaseFlatRowConverter converter = new HBaseFlatRowConverter(conf, rowType);

        Assert.assertEquals("stu:id", converter.getCommonConf().getColumn().get(0).getName());

        Mutation toExternal = converter.toExternal(rowData, null);
        Assert.assertFalse(toExternal.getFamilyCellMap().isEmpty());
    }

    /**
     * Converts the Java type used for UDF parameters of SQL DATE type ({@link java.sql.Date}) to
     * internal representation (int).
     */
    public static int dateToInternal(java.sql.Date date) {
        long ts = date.getTime() + LOCAL_TZ.getOffset(date.getTime());
        return (int) (ts / MILLIS_PER_DAY);
    }

    /**
     * Converts the Java type used for UDF parameters of SQL TIME type ({@link java.sql.Time}) to
     * internal representation (int).
     */
    public static int timeToInternal(java.sql.Time time) {
        long ts = time.getTime() + LOCAL_TZ.getOffset(time.getTime());
        return (int) (ts % MILLIS_PER_DAY);
    }
}
