/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.chunjun.connector.hdfs.source;

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.connector.hdfs.conf.HdfsConf;
import com.dtstack.chunjun.connector.hdfs.converter.HdfsRawTypeConverter;
import com.dtstack.chunjun.connector.hdfs.converter.HdfsTextColumnConverter;
import com.dtstack.chunjun.connector.hdfs.converter.HdfsTextRowConverter;
import com.dtstack.chunjun.constants.ConstantValue;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.converter.IDeserializationConverter;
import com.dtstack.chunjun.source.format.BaseRichInputFormat;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.util.FileSystemUtil;
import com.dtstack.chunjun.util.PluginUtil;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.core.io.InputSplit;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Date: 2021/06/08 Company: www.dtstack.com
 *
 * @author tudou
 */
public abstract class BaseHdfsInputFormat extends BaseRichInputFormat {

    protected HdfsConf hdfsConf;

    /** the key to read data into */
    protected Object key;
    /** the value to read data into */
    protected Object value;

    protected boolean openKerberos;

    protected transient FileSystem fs;
    protected transient UserGroupInformation ugi;
    protected transient org.apache.hadoop.mapred.InputFormat inputFormat;
    protected transient JobConf hadoopJobConf;
    protected transient RecordReader recordReader;

    protected List<FieldConf> partitionColumnList;
    protected AbstractRowConverter partitionConverter;
    protected List<Object> partitionColumnValueList;

    @Override
    public InputSplit[] createInputSplitsInternal(int minNumSplits) throws IOException {
        openKerberos = FileSystemUtil.isOpenKerberos(hdfsConf.getHadoopConfig());
        if (openKerberos) {
            DistributedCache distributedCache =
                    PluginUtil.createDistributedCacheFromContextClassLoader();
            UserGroupInformation ugi =
                    FileSystemUtil.getUGI(
                            hdfsConf.getHadoopConfig(), hdfsConf.getDefaultFS(), distributedCache);
            LOG.info("user:{}, ", ugi.getShortUserName());
            return ugi.doAs(
                    (PrivilegedAction<InputSplit[]>)
                            () -> {
                                try {
                                    return createHdfsSplit(minNumSplits);
                                } catch (Exception e) {
                                    throw new ChunJunRuntimeException(
                                            "error to create hdfs splits", e);
                                }
                            });
        } else {
            return createHdfsSplit(minNumSplits);
        }
    }

    @Override
    public void openInputFormat() throws IOException {
        super.openInputFormat();
        initHadoopJobConf();
        this.inputFormat = createInputFormat();
        openKerberos = FileSystemUtil.isOpenKerberos(hdfsConf.getHadoopConfig());
        if (openKerberos) {
            ugi =
                    FileSystemUtil.getUGI(
                            hdfsConf.getHadoopConfig(),
                            hdfsConf.getDefaultFS(),
                            getRuntimeContext().getDistributedCache());
        }
        this.partitionColumnList = hdfsConf.getPartitionColumnList();
        this.partitionColumnValueList = new ArrayList<>(partitionColumnList.size());
        if (useAbstractColumn) {
            this.partitionConverter = new HdfsTextColumnConverter(partitionColumnList);
        } else {
            this.partitionConverter =
                    new HdfsTextRowConverter(
                            TableUtil.createRowType(
                                    partitionColumnList, HdfsRawTypeConverter::apply));
        }
    }

    protected void initPartitionColumnValue(String currentFilePath) {
        partitionColumnValueList.clear();
        ArrayList<IDeserializationConverter> toInternalConverterList;
        try {
            Field toInternalConvertersField =
                    AbstractRowConverter.class.getDeclaredField("toInternalConverters");
            toInternalConvertersField.setAccessible(true);
            toInternalConverterList =
                    (ArrayList<IDeserializationConverter>)
                            toInternalConvertersField.get(partitionConverter);
        } catch (Exception e) {
            throw new ChunJunRuntimeException("failed to get partition toInternalConverters");
        }
        for (int i = 0; i < partitionColumnList.size(); i++) {
            FieldConf partitionFieldConf = partitionColumnList.get(i);
            String partitionName = partitionFieldConf.getName();
            int partitionIndex = currentFilePath.lastIndexOf(partitionName);
            int equalSymbolIndex =
                    currentFilePath.indexOf(ConstantValue.EQUAL_SYMBOL, partitionIndex);
            if (equalSymbolIndex == -1) {
                throw new ChunJunRuntimeException(
                        String.format(
                                "failed to get hdfs partition value,currentFilePath=%s,partitionName=%s",
                                currentFilePath, partitionName));
            }
            int slashIndex = currentFilePath.indexOf(File.separator, partitionIndex);
            slashIndex = slashIndex == -1 ? currentFilePath.length() : slashIndex;
            String partitionValueStr = currentFilePath.substring(equalSymbolIndex + 1, slashIndex);
            try {
                this.partitionColumnValueList.add(
                        toInternalConverterList.get(i).deserialize(partitionValueStr));
            } catch (Exception e) {
                throw new ChunJunRuntimeException(
                        String.format(
                                "failed to get deserialize path=%s,partitionValueStr=%s,partitionFieldConf=%s",
                                currentFilePath, partitionValueStr, partitionFieldConf),
                        e);
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean reachedEnd() throws IOException {
        return !recordReader.next(key, value);
    }

    @Override
    public void closeInternal() throws IOException {
        if (recordReader != null) {
            recordReader.close();
        }
    }

    /** init Hadoop Job Config */
    protected void initHadoopJobConf() {
        hadoopJobConf =
                FileSystemUtil.getJobConf(hdfsConf.getHadoopConfig(), hdfsConf.getDefaultFS());
        hadoopJobConf.set(HdfsPathFilter.KEY_REGEX, hdfsConf.getFilterRegex());
        FileSystemUtil.setHadoopUserName(hadoopJobConf);
    }

    /**
     * create hdfs data splits
     *
     * @param minNumSplits
     * @return
     * @throws IOException
     */
    public abstract InputSplit[] createHdfsSplit(int minNumSplits) throws IOException;

    /**
     * create hdfs inputFormat
     *
     * @return org.apache.hadoop.mapred.InputFormat
     */
    public abstract org.apache.hadoop.mapred.InputFormat createInputFormat();

    public void setHdfsConf(HdfsConf hdfsConf) {
        this.hdfsConf = hdfsConf;
    }
}
