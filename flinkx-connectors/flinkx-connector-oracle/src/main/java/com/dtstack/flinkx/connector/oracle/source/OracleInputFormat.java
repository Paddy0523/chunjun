package com.dtstack.flinkx.connector.oracle.source;

import com.dtstack.flinkx.connector.jdbc.source.JdbcInputFormat;
import com.dtstack.flinkx.connector.jdbc.util.JdbcUtil;

import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * company www.dtstack.com
 *
 * @author jier
 */
public class OracleInputFormat extends JdbcInputFormat {

    /**
     * 构建时间边界字符串
     *
     * @param location 边界位置(起始/结束)
     * @return
     */
    @Override
    protected String getTimeStr(Long location) {
        String timeStr;
        Timestamp ts = new Timestamp(JdbcUtil.getMillis(location));
        ts.setNanos(JdbcUtil.getNanos(location));
        timeStr = JdbcUtil.getNanosTimeStr(ts.toString());
        timeStr = timeStr.substring(0, 23);
        timeStr = String.format("TO_TIMESTAMP('%s','yyyy-MM-dd HH24:mi:ss.FF6')", timeStr);

        return timeStr;
    }

    @Override
    protected void queryForPolling(String startLocation) throws SQLException {
        // 每隔五分钟打印一次，(当前时间 - 任务开始时间) % 300秒 <= 一个间隔轮询周期
        if ((System.currentTimeMillis() - startTime) % 300000 <= jdbcConf.getPollingInterval()) {
            LOG.info("polling startLocation = {}", startLocation);
        } else {
            LOG.debug("polling startLocation = {}", startLocation);
        }

        boolean isNumber = StringUtils.isNumeric(startLocation);
        switch (type) {
            case TIMESTAMP:
            case DATE:
                Timestamp ts =
                        isNumber
                                ? new Timestamp(Long.parseLong(startLocation))
                                : Timestamp.valueOf(startLocation);
                ps.setTimestamp(1, ts);
                break;
            default:
                if (isNumber) {
                    ps.setLong(1, Long.parseLong(startLocation));
                } else {
                    ps.setString(1, startLocation);
                }
        }
        resultSet = ps.executeQuery();
        hasNext = resultSet.next();
    }
}
