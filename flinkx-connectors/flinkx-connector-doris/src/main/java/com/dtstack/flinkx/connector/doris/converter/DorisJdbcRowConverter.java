package com.dtstack.flinkx.connector.doris.converter;

import com.dtstack.flinkx.connector.jdbc.converter.JdbcRowConverter;

import org.apache.flink.table.types.logical.RowType;

public class DorisJdbcRowConverter extends JdbcRowConverter {
    public DorisJdbcRowConverter(RowType rowType) {
        super(rowType);
    }
}
