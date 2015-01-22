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

import com.hazelcast.core.ManagedContext;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableContext;
import com.hazelcast.nio.serialization.SerializationService;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.regex.Pattern;

final class PortableContextImpl implements PortableContext {

    private static final Pattern NESTED_FIELD_PATTERN = Pattern.compile("\\.");

    private final SerializationService serializationService;

    PortableContextImpl(SerializationService serializationService, int version) {
        this.serializationService = serializationService;
    }

    @Override
    public int getClassVersion(int factoryId, int classId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setClassVersion(int factoryId, int classId, int version) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ClassDefinition lookupClassDefinition(int factoryId, int classId, int version) {
        throw new UnsupportedOperationException();
    }

    public ClassDefinition lookupClassDefinition(Data data) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ClassDefinition registerClassDefinition(final ClassDefinition cd) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ClassDefinition lookupOrRegisterClassDefinition(Portable p) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public FieldDefinition getFieldDefinition(ClassDefinition classDef, String name) {
        FieldDefinition fd = classDef.getField(name);
        if (fd == null) {
            String[] fieldNames = NESTED_FIELD_PATTERN.split(name);
            if (fieldNames.length > 1) {
                ClassDefinition currentClassDef = classDef;
                for (int i = 0; i < fieldNames.length; i++) {
                    name = fieldNames[i];
                    fd = currentClassDef.getField(name);
                    if (i == fieldNames.length - 1) {
                        break;
                    }
                    if (fd == null) {
                        throw new IllegalArgumentException("Unknown field: " + name);
                    }
                    currentClassDef = lookupClassDefinition(fd.getFactoryId(), fd.getClassId(),
                            currentClassDef.getVersion());
                    if (currentClassDef == null) {
                        throw new IllegalArgumentException("Not a registered Portable field: " + fd);
                    }
                }
            }
        }
        return fd;
    }

    public int getVersion() {
        return 0;
    }

    public ManagedContext getManagedContext() {
        return serializationService.getManagedContext();
    }

    @Override
    public ByteOrder getByteOrder() {
        return serializationService.getByteOrder();
    }


    @Override
    public boolean hasClassDefinition(Data data) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ClassDefinition[] getClassDefinitions(Data data) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ClassDefinition createClassDefinition(int factoryId, byte[] binary) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object readField(Data data, String fieldName) throws NoSuchFieldException {
        try {
            BufferObjectDataInput in = serializationService.createObjectDataInput(data);
            in.readInt(); // factory-id
            in.readInt(); // class-id
            in.readInt(); // end pos

            while (true) {
                short len = in.readShort();
                if (len != fieldName.length()) {
                    in.skipBytes(len + 1);
                    int next = in.readInt();
                    if (next < 0) {
                        break;
                    }
                    in.position(next);
                } else {
                    char[] chars = new char[len];
                    for (int k = 0; k < len; k++) {
                        chars[k] = (char) in.readUnsignedByte();
                    }
                    String name = new String(chars);
                    byte typeId = in.readByte();
                    if (name.equals(fieldName)) {
                        in.skipBytes(4); // skip next

                        FieldType type = FieldType.get(typeId);
                        switch (type) {
                            case BYTE:
                                return in.readByte();
                            case BOOLEAN:
                                return in.readBoolean();
                            case CHAR:
                                return in.readChar();
                            case SHORT:
                                return in.readShort();
                            case INT:
                                return in.readInt();
                            case LONG:
                                return in.readLong();
                            case FLOAT:
                                return in.readFloat();
                            case DOUBLE:
                                return in.readDouble();
                            case UTF:
                                return in.readUTF();
                            default:
                                throw new IllegalArgumentException();
                        }
                    } else {
                        int next = in.readInt();
                        if (next < 0) {
                            break;
                        }
                        in.position(next);
                    }
                }
            }

        } catch (IOException e) {
            throw new HazelcastSerializationException(e);
        }
        throw new NoSuchFieldException(fieldName);
    }
}
