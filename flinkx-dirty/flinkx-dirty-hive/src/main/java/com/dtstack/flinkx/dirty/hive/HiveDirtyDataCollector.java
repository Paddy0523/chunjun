/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.dirty.hive;

import org.apache.flink.api.common.cache.DistributedCache;

import com.dtstack.flinkx.dirty.DirtyConf;
import com.dtstack.flinkx.dirty.consumer.DirtyDataCollector;
import com.dtstack.flinkx.dirty.impl.DirtyDataEntry;
import com.dtstack.flinkx.throwable.FlinkxRuntimeException;
import com.dtstack.flinkx.util.DateUtil;
import com.dtstack.flinkx.util.FileSystemUtil;
import com.dtstack.flinkx.util.GsonUtil;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.EnumSet;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class HiveDirtyDataCollector extends DirtyDataCollector {

    private static final Logger LOG = LoggerFactory.getLogger(HiveDirtyDataCollector.class);

    private static final String FIELD_DELIMITER = "\u0001";

    private static final String LINE_DELIMITER = "\n";

    private static final String PATH_KEY = "hive.path";

    private static final String CONF_KEY = "hive.hadoopconfig";

    private static final String ERROR_TYPE = "Dirty Exception";

    private final Gson gson = new GsonBuilder().disableHtmlEscaping().create();

    private final EnumSet<HdfsDataOutputStream.SyncFlag> syncFlags =
            EnumSet.of(HdfsDataOutputStream.SyncFlag.UPDATE_LENGTH);

    private FSDataOutputStream stream;

    /** hadoop conf of hive. */
    private Map<String, Object> config;

    /** the dirty data path. */
    private String path;

    @Override
    protected void init(DirtyConf conf) {
        Properties pluginProperties = conf.getPluginProperties();
        path = pluginProperties.getProperty(PATH_KEY) + "/" + UUID.randomUUID() + ".txt";
        config =
                (Map<String, Object>)
                        GsonUtil.GSON.fromJson(pluginProperties.getProperty(CONF_KEY), Map.class);

        check(path, config);
        if (LOG.isDebugEnabled()) {
            LOG.debug("The Path of Hive Dirty Plugin is: " + path);
        }
    }

    private void check(String path, Map<String, Object> hadoopConfig) {
        StringBuilder builder = new StringBuilder();
        if (StringUtils.isEmpty(path)) {
            builder.append("No [path] supplied for Hive Dirty Plugin\n");
        }

        if (MapUtils.isEmpty(hadoopConfig)) {
            builder.append("No [hadoopConfig] supplied for Hive Dirty Plugin\n");
        }

        if (builder.length() > 0) {
            throw new FlinkxRuntimeException("Init Hive Dirty Plugin failed. Cause by: " + builder);
        }
    }

    @Override
    public void open(DistributedCache distributedCache) {
        try {
            FileSystem fs = FileSystemUtil.getFileSystem(config, null, distributedCache);
            Path location = new Path(this.path);
            stream = fs.create(location, true);
            LOG.info("Hive Dirty Plugin Open succeed.");
        } catch (Exception e) {
            throw new FlinkxRuntimeException("Open hive dirty manager error", e);
        }
    }

    @Override
    public void close() {
        if (stream != null) {
            try {
                stream.flush();
                stream.close();
            } catch (IOException e) {
                throw new FlinkxRuntimeException(e);
            }
        }
    }

    @Override
    protected void consume(DirtyDataEntry dirty) throws Exception {
        String content = dirty.getDirtyContent();
        String msg = gson.toJson(dirty.getErrorMessage());
        String line =
                StringUtils.join(
                        new String[] {
                            content, ERROR_TYPE, msg, DateUtil.timestampToString(new Date())
                        },
                        FIELD_DELIMITER);

        stream.write(line.getBytes(StandardCharsets.UTF_8));
        stream.write(LINE_DELIMITER.getBytes(StandardCharsets.UTF_8));
        DFSOutputStream dfsOutputStream = (DFSOutputStream) stream.getWrappedStream();
        dfsOutputStream.hsync(syncFlags);
    }
}
