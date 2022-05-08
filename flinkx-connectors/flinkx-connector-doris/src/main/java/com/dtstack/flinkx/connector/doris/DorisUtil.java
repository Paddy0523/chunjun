package com.dtstack.flinkx.connector.doris;

import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;

public class DorisUtil {

    private static final String JDBC_QUERY_PORT = "9030";

    private static final String JDBC_TEMPLATE = "jdbc:mysql://%s:%s?useSSL=false";

    /**
     * split fe and get fe ip. like: http://172.16.21.193:8030
     *
     * @param fe fe
     * @return ip of fe.
     */
    private static String splitFe(String fe) {
        String[] split = fe.split("://");
        for (String s : split) {
            String[] items = s.split(":");
            if (items.length == 2) {
                return items[0];
            }
        }
        throw new RuntimeException("Get fe ip from fe uri: " + fe + " failed.");
    }

    public static String getJdbcUrlFromFe(List<String> feNodes, String url) {
        if (StringUtils.isEmpty(url)) {
            Collections.shuffle(feNodes);
            String fe = feNodes.get(0);
            String feIp = splitFe(fe);
            return String.format(JDBC_TEMPLATE, feIp, JDBC_QUERY_PORT);
        }
        return url;
    }
}
