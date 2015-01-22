/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableContext;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.serialization.v1.V1SerializationServiceBuilder;
import com.hazelcast.nio.serialization.v2.V2SerializationServiceBuilder;
import com.hazelcast.nio.serialization.v3.V3SerializationServiceBuilder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@Warmup(iterations = 1, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 3, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(Threads.MAX)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class PortableBenchmark {

    SerializationService serializationService1 = createV1();
    SerializationService serializationService2 = createV2();
    SerializationService serializationService3 = createV3();

    Portable portable;
    Data dataV1;
    Data dataV2;
    Data dataV3;

    @Setup
    public void setUp() {
//        portable = new PortablePerson((int) (Math.random() * 111), System.currentTimeMillis(),
//                "name last-name " + System.nanoTime(),
//                new PortableAddress("street " + System.nanoTime(), (int) (Math.random() * 1111)));

        portable = new ComplexPortable(new Object());

        dataV1 = serializationService1.toData(portable);
        dataV2 = serializationService2.toData(portable);
        dataV3 = serializationService3.toData(portable);
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    public void writeV1() {
        serializationService1.toData(portable);
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    public void writeV2() {
        serializationService2.toData(portable);
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    public void writeV3() {
        serializationService3.toData(portable);
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    public void readV1() {
        serializationService1.toObject(dataV1);
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    public void readV2() {
        serializationService2.toObject(dataV2);
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    public void readV3() {
        serializationService3.toObject(dataV3);
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    public void querySimpleV1() throws IOException, NoSuchFieldException {
        PortableContext context = serializationService1.getPortableContext();
//        context.readField(dataV1, "age");
//        context.readField(dataV1, "name");
        context.readField(dataV1, "long22");
        context.readField(dataV1, "bool1");
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    public void querySimpleV2() throws IOException, NoSuchFieldException {
        PortableContext context = serializationService2.getPortableContext();
//        context.readField(dataV2, "age");
//        context.readField(dataV2, "name");
        context.readField(dataV2, "long22");
        context.readField(dataV2, "bool1");
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    public void querySimpleV3() throws IOException, NoSuchFieldException {
        PortableContext context = serializationService3.getPortableContext();
//        context.readField(dataV3, "age");
//        context.readField(dataV3, "name");
        context.readField(dataV3, "long22");
        context.readField(dataV3, "bool1");
    }

//    @Benchmark
//    @BenchmarkMode({Mode.Throughput})
//    public void queryNestedV1() throws IOException, NoSuchFieldException {
//        PortableContext context = serializationService1.getPortableContext();
//        context.readField(dataV1, "address.street");
//    }
//
//    @Benchmark
//    @BenchmarkMode({Mode.Throughput})
//    public void queryNestedV2() throws IOException, NoSuchFieldException {
//        PortableContext context = serializationService2.getPortableContext();
//        context.readField(dataV2, "address.street");
//    }
//
//    @Benchmark
//    @BenchmarkMode({Mode.Throughput})
//    public void queryNestedV3() throws IOException, NoSuchFieldException {
//        PortableContext context = serializationService3.getPortableContext();
//        context.readField(dataV3, "address.street");
//    }

    private static SerializationService createV1() {
        return new V1SerializationServiceBuilder()
                .setAllowUnsafe(true)
                .setUseNativeByteOrder(true)
                .addPortableFactory(1, new TestPortableFactory())
                .build();
    }

    private static SerializationService createV2() {
        return new V2SerializationServiceBuilder()
                .setAllowUnsafe(true)
                .setUseNativeByteOrder(true)
                .addPortableFactory(1, new TestPortableFactory())
                .build();
    }

    private static SerializationService createV3() {
        return new V3SerializationServiceBuilder()
                .setAllowUnsafe(true)
                .setUseNativeByteOrder(true)
                .addPortableFactory(1, new TestPortableFactory())
                .build();
    }

}
