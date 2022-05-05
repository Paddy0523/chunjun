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

package com.dtstack.flinkx.connector.hbase2.table.lookup;

import com.dtstack.flinkx.connector.hbase2.common.HBaseTableSchema;
import com.dtstack.flinkx.connector.hbase2.conf.HBaseConf;
import com.dtstack.flinkx.connector.hbase2.converter.AsyncHBaseSerde;
import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.enums.ECacheContentType;
import com.dtstack.flinkx.factory.FlinkxThreadFactory;
import com.dtstack.flinkx.lookup.AbstractLruTableFunction;
import com.dtstack.flinkx.lookup.cache.CacheMissVal;
import com.dtstack.flinkx.lookup.cache.CacheObj;
import com.dtstack.flinkx.lookup.conf.LookupConf;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;

import com.google.common.collect.Maps;
import com.stumbleupon.async.Deferred;
import org.hbase.async.Config;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class HBaseLruTableFunction extends AbstractLruTableFunction {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(HBaseLruTableFunction.class);
    private static final int DEFAULT_BOSS_THREADS = 1;
    private static final int DEFAULT_IO_THREADS = Runtime.getRuntime().availableProcessors() * 2;
    private static final int DEFAULT_POOL_SIZE = DEFAULT_IO_THREADS + DEFAULT_BOSS_THREADS;
    private transient HBaseClient hBaseClient;

    private final HBaseTableSchema hbaseTableSchema;

    private transient AsyncHBaseSerde serde;

    private final HBaseConf hBaseConf;

    public HBaseLruTableFunction(
            LookupConf lookupConf,
            HBaseTableSchema hbaseTableSchema,
            HBaseConf hBaseConf,
            AbstractRowConverter rowConverter) {
        super(lookupConf, rowConverter);
        this.hBaseConf = hBaseConf;
        this.hbaseTableSchema = hbaseTableSchema;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        Config asyncClientConfig = new Config();
        for (Map.Entry<String, Object> entry : hBaseConf.getHbaseConfig().entrySet()) {
            asyncClientConfig.overrideConfig(entry.getKey(), entry.getValue().toString());
        }
        this.serde = new AsyncHBaseSerde(hbaseTableSchema, hBaseConf.getNullMode());
        ExecutorService executorService =
                new ThreadPoolExecutor(
                        DEFAULT_POOL_SIZE,
                        DEFAULT_POOL_SIZE,
                        0L,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(),
                        new FlinkxThreadFactory("hbase-async"));

        hBaseClient = new HBaseClient(asyncClientConfig, executorService);
        CheckResult result;
        try {
            Deferred<CheckResult> deferred =
                    hBaseClient
                            .ensureTableExists(hbaseTableSchema.getTableName())
                            .addCallbacks(
                                    arg -> new CheckResult(true, ""),
                                    arg -> new CheckResult(false, arg.toString()));

            result = deferred.join();
        } catch (Exception e) {
            throw new RuntimeException("create hbase connection fail:", e);
        }
        if (!result.isConnect()) {
            throw new RuntimeException(result.getExceptionMsg());
        }
    }

    @Override
    public void handleAsyncInvoke(
            CompletableFuture<Collection<RowData>> future, Object... rowKeys) {
        Object rowKey = rowKeys[0];
        byte[] key = serde.getRowKey(rowKey);
        String keyStr = new String(key);
        GetRequest getRequest = new GetRequest(hbaseTableSchema.getTableName(), key);
        hBaseClient
                .get(getRequest)
                .addCallbacks(
                        keyValues -> {
                            try {
                                Map<String, Map<String, byte[]>> sideMap = Maps.newHashMap();
                                for (KeyValue keyValue : keyValues) {
                                    String cf = new String(keyValue.family());
                                    String col = new String(keyValue.qualifier());
                                    if (!sideMap.containsKey(cf)) {
                                        Map<String, byte[]> cfMap = Maps.newHashMap();
                                        cfMap.put(col, keyValue.value());
                                        sideMap.put(cf, cfMap);
                                    } else {
                                        sideMap.get(cf).putIfAbsent(col, keyValue.value());
                                    }
                                }
                                RowData rowData = serde.convertToNewRow(sideMap, key);
                                if (keyValues.size() > 0) {
                                    try {
                                        if (openCache()) {
                                            sideCache.putCache(
                                                    keyStr,
                                                    CacheObj.buildCacheObj(
                                                            ECacheContentType.MultiLine,
                                                            Collections.singletonList(rowData)));
                                        }
                                        future.complete(Collections.singletonList(rowData));
                                    } catch (Exception e) {
                                        future.completeExceptionally(e);
                                    }
                                } else {
                                    dealMissKey(future);
                                    if (openCache()) {
                                        sideCache.putCache(keyStr, CacheMissVal.getMissKeyObj());
                                    }
                                }
                            } catch (Exception e) {
                                future.completeExceptionally(e);
                                LOG.error("record:" + keyStr);
                                LOG.error("get side record exception:", e);
                            }
                            return "";
                        },
                        o -> {
                            LOG.error("record:" + keyStr);
                            LOG.error("get side record exception:" + o);
                            future.complete(Collections.EMPTY_LIST);
                            return "";
                        });
    }

    @Override
    public void close() throws Exception {
        super.close();
        hBaseClient.shutdown();
    }

    static class CheckResult {
        private final boolean connect;

        private final String exceptionMsg;

        CheckResult(boolean connect, String msg) {
            this.connect = connect;
            this.exceptionMsg = msg;
        }

        public boolean isConnect() {
            return connect;
        }

        public String getExceptionMsg() {
            return exceptionMsg;
        }
    }
}
