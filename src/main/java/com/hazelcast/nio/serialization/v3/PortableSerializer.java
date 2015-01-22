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
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.SerializationConstants;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

final class PortableSerializer implements StreamSerializer<Portable> {

    private final PortableContextImpl context;
    private final Map<Integer, PortableFactory> factories = new HashMap<Integer, PortableFactory>();

    PortableSerializer(PortableContextImpl context, Map<Integer, ? extends PortableFactory> portableFactories) {
        this.context = context;
        factories.putAll(portableFactories);
    }

    public int getTypeId() {
        return SerializationConstants.CONSTANT_TYPE_PORTABLE;
    }

    public void write(ObjectDataOutput out, Portable p) throws IOException {
        if (!(out instanceof BufferObjectDataOutput)) {
            throw new IllegalArgumentException("ObjectDataOutput must be instance of BufferObjectDataOutput!");
        }
        if (p.getClassId() == 0) {
            throw new IllegalArgumentException("Portable class id cannot be zero!");
        }

        out.writeInt(p.getFactoryId());
        out.writeInt(p.getClassId());
        writeInternal((BufferObjectDataOutput) out, p);
    }

    void writeInternal(BufferObjectDataOutput out, Portable p) throws IOException {
        DefaultPortableWriter writer = new DefaultPortableWriter(this, out);
        p.writePortable(writer);
        writer.end();
    }

    public Portable read(ObjectDataInput in) throws IOException {
        if (!(in instanceof BufferObjectDataInput)) {
            throw new IllegalArgumentException("ObjectDataInput must be instance of BufferObjectDataInput!");
        }

        int factoryId = in.readInt();
        int classId = in.readInt();

        Portable portable = createNewPortableInstance(factoryId, classId);
        DefaultPortableReader reader = createReader((BufferObjectDataInput) in);
        portable.readPortable(reader);
        reader.end();
        return portable;
    }

    private Portable createNewPortableInstance(int factoryId, int classId) {
        final PortableFactory portableFactory = factories.get(factoryId);
        if (portableFactory == null) {
            throw new HazelcastSerializationException("Could not find PortableFactory for factory-id: " + factoryId);
        }
        final Portable portable = portableFactory.create(classId);
        if (portable == null) {
            throw new HazelcastSerializationException("Could not create Portable for class-id: " + classId);
        }
        return portable;
    }

    Portable readAndInitialize(BufferObjectDataInput in) throws IOException {
        Portable p = read(in);
        final ManagedContext managedContext = context.getManagedContext();
        return managedContext != null ? (Portable) managedContext.initialize(p) : p;
    }

    DefaultPortableReader createReader(BufferObjectDataInput in) throws IOException {
        return new DefaultPortableReader(this, in);
    }

    public void destroy() {
        factories.clear();
    }
}

