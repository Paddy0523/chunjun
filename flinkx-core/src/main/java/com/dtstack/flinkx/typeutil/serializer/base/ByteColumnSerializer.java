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

package com.dtstack.flinkx.typeutil.serializer.base;

import com.dtstack.flinkx.element.AbstractBaseColumn;
import com.dtstack.flinkx.element.column.ByteColumn;
import com.dtstack.flinkx.element.column.NullColumn;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/** @author liuliu 2022/5/12 */
public class ByteColumnSerializer extends TypeSerializerSingleton<AbstractBaseColumn> {

    private static final long serialVersionUID = 1L;

    /** Sharable instance of the StringColumnSerializer. */
    public static final ByteColumnSerializer INSTANCE = new ByteColumnSerializer();

    private static final ByteColumn EMPTY = new ByteColumn((byte) 0);

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public AbstractBaseColumn createInstance() {
        return EMPTY;
    }

    @Override
    public AbstractBaseColumn copy(AbstractBaseColumn from) {
        return from;
    }

    @Override
    public AbstractBaseColumn copy(AbstractBaseColumn from, AbstractBaseColumn reuse) {
        return from;
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(AbstractBaseColumn record, DataOutputView target) throws IOException {
        if (record instanceof NullColumn) {
            target.writeBoolean(false);
        } else {
            target.writeBoolean(true);
            target.writeByte((byte) record.getData());
        }
    }

    @Override
    public AbstractBaseColumn deserialize(DataInputView source) throws IOException {
        boolean isNotNull = source.readBoolean();
        if (isNotNull) {
            return new ByteColumn(source.readByte());
        } else {
            return new NullColumn();
        }
    }

    @Override
    public AbstractBaseColumn deserialize(AbstractBaseColumn reuse, DataInputView source)
            throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        boolean isNotNull = source.readBoolean();
        target.writeBoolean(isNotNull);
        if (isNotNull) {
            target.writeByte(source.readByte());
        }
    }

    @Override
    public TypeSerializerSnapshot<AbstractBaseColumn> snapshotConfiguration() {
        return new ByteColumnSerializerSnapshot();
    }

    /** Serializer configuration snapshot for compatibility and format evolution. */
    @SuppressWarnings("WeakerAccess")
    public static final class ByteColumnSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<AbstractBaseColumn> {

        public ByteColumnSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
