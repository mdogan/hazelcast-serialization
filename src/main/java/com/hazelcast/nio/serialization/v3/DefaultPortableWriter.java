/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.nio.serialization.v3;

import com.hazelcast.nio.Bits;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class DefaultPortableWriter implements PortableWriter {

    private final PortableSerializer serializer;
    private final BufferObjectDataOutput out;
    private final int begin;
    private final Set<String> writtenFields;
    private int next = -1;
    private boolean raw;

    public DefaultPortableWriter(PortableSerializer serializer, BufferObjectDataOutput out)
            throws IOException {
        this.serializer = serializer;
        this.out = out;
        this.writtenFields = new HashSet<String>();
        this.begin = out.position();

        // room for final offset
        out.writeZeroBytes(4);
    }

    public void writeInt(String fieldName, int value) throws IOException {
        writeName(fieldName, FieldType.INT);
        out.writeInt(value);
    }

    public void writeLong(String fieldName, long value) throws IOException {
        writeName(fieldName, FieldType.LONG);
        out.writeLong(value);
    }

    public void writeUTF(String fieldName, String str) throws IOException {
        writeName(fieldName, FieldType.UTF);
        out.writeUTF(str);
    }

    public void writeBoolean(String fieldName, boolean value) throws IOException {
        writeName(fieldName, FieldType.BOOLEAN);
        out.writeBoolean(value);
    }

    public void writeByte(String fieldName, byte value) throws IOException {
        writeName(fieldName, FieldType.BYTE);
        out.writeByte(value);
    }

    public void writeChar(String fieldName, int value) throws IOException {
        writeName(fieldName, FieldType.CHAR);
        out.writeChar(value);
    }

    public void writeDouble(String fieldName, double value) throws IOException {
        writeName(fieldName, FieldType.DOUBLE);
        out.writeDouble(value);
    }

    public void writeFloat(String fieldName, float value) throws IOException {
        writeName(fieldName, FieldType.FLOAT);
        out.writeFloat(value);
    }

    public void writeShort(String fieldName, short value) throws IOException {
        writeName(fieldName, FieldType.SHORT);
        out.writeShort(value);
    }

    public void writePortable(String fieldName, Portable portable) throws IOException {
        writeName(fieldName, FieldType.PORTABLE);
        final boolean isNull = portable == null;
        out.writeBoolean(isNull);

        if (!isNull) {
            out.writeInt(portable.getFactoryId());
            out.writeInt(portable.getClassId());

            // TODO
            serializer.writeInternal(out, portable);
        }
    }


    public void writeNullPortable(String fieldName, int factoryId, int classId) throws IOException {
        writeName(fieldName, FieldType.PORTABLE);
        out.writeBoolean(true);
    }

    public void writeByteArray(String fieldName, byte[] values) throws IOException {
        writeName(fieldName, FieldType.BYTE_ARRAY);
        out.writeByteArray(values);
    }

    public void writeCharArray(String fieldName, char[] values) throws IOException {
        writeName(fieldName, FieldType.CHAR_ARRAY);
        out.writeCharArray(values);
    }

    public void writeIntArray(String fieldName, int[] values) throws IOException {
        writeName(fieldName, FieldType.INT_ARRAY);
        out.writeIntArray(values);
    }

    public void writeLongArray(String fieldName, long[] values) throws IOException {
        writeName(fieldName, FieldType.LONG_ARRAY);
        out.writeLongArray(values);
    }

    public void writeDoubleArray(String fieldName, double[] values) throws IOException {
        writeName(fieldName, FieldType.DOUBLE_ARRAY);
        out.writeDoubleArray(values);
    }

    public void writeFloatArray(String fieldName, float[] values) throws IOException {
        writeName(fieldName, FieldType.FLOAT_ARRAY);
        out.writeFloatArray(values);
    }

    public void writeShortArray(String fieldName, short[] values) throws IOException {
        writeName(fieldName, FieldType.SHORT_ARRAY);
        out.writeShortArray(values);
    }

    public void writePortableArray(String fieldName, Portable[] portables) throws IOException {
        writeName(fieldName, FieldType.PORTABLE_ARRAY);
        final int len = portables == null ? 0 : portables.length;
        out.writeInt(len);


        if (len > 0) {
            Portable p0 = portables[0];

            out.writeInt(p0.getFactoryId());
            out.writeInt(p0.getClassId());

            // TODO
            final int offset = out.position();
            out.writeZeroBytes(len * 4);
            for (int i = 0; i < portables.length; i++) {
                Portable portable = portables[i];
                checkPortableAttributes(p0, portable);
                int position = out.position();
                out.writeInt(offset + i * Bits.INT_SIZE_IN_BYTES, position);
                serializer.writeInternal(out, portable);
            }
        }
    }

    private void checkPortableAttributes(Portable p0, Portable portable) {
        if (p0.getFactoryId() != portable.getFactoryId()) {
            throw new HazelcastSerializationException("Wrong Portable type! Generic portable types are not supported! "
                    + " Expected factory-id: " + p0.getFactoryId() + ", Actual factory-id: " + portable.getFactoryId());
        }
        if (p0.getClassId() != portable.getClassId()) {
            throw new HazelcastSerializationException("Wrong Portable type! Generic portable types are not supported! "
                    + "Expected class-id: " + p0.getClassId() + ", Actual class-id: " + portable.getClassId());
        }
    }

    private void writeName(String fieldName, FieldType fieldType) throws IOException {
        if (writtenFields.add(fieldName)) {
            int position = out.position();
            out.writeShort(fieldName.length());
            out.writeBytes(fieldName);
            out.writeByte(fieldType.getId());
            if (next > -1) {
                out.writeInt(next, position);
            }
            next = out.position();
            out.writeInt(-1); // next space
        } else {
            throw new HazelcastSerializationException("Field '" + fieldName + "' has already been written!");
        }
    }


    public ObjectDataOutput getRawDataOutput() throws IOException {
        throw new UnsupportedOperationException();
    }

    void end() throws IOException {
        // write final offset
        int position = out.position();
        out.writeInt(begin, position);
    }
}
