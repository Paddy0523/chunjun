package com.dtstack.flinkx.connector.doris.source;

import com.dtstack.flinkx.connector.doris.options.DorisConf;
import com.dtstack.flinkx.connector.jdbc.source.JdbcInputFormat;
import com.dtstack.flinkx.connector.jdbc.source.JdbcInputFormatBuilder;

import org.apache.commons.lang3.StringUtils;

import java.util.List;

public class DorisInputFormatBuilder extends JdbcInputFormatBuilder {
    public DorisInputFormatBuilder(JdbcInputFormat format) {
        super(format);
    }

    @Override
    protected void checkFormat() {
        DorisInputFormat format = (DorisInputFormat) this.format;
        DorisConf config = format.getDorisConf();
        List<String> feNodes = config.getFeNodes();
        String url = config.getUrl();

        if (StringUtils.isEmpty(url) && (null == feNodes || feNodes.isEmpty())) {
            throw new IllegalArgumentException(
                    "Choose one of 'url' and 'feNodes', them can not be empty at same time.");
        }
    }
}
