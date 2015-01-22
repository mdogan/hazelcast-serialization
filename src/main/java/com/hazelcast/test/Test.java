package com.hazelcast.test;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.serialization.v3.V3SerializationServiceBuilder;

public class Test {

    static final short FACTORY_ID = 1;

    public static void main(String[] args) throws Exception {
        PortableFactory portableFactory = new TestPortableFactory();

        SerializationService ss = new V3SerializationServiceBuilder()
                .addPortableFactory(FACTORY_ID, portableFactory).build();

//        Portable portable = new PortablePerson(123456, System.currentTimeMillis(), "some name surname",
//                new PortableAddress("some street name", 987));

        Portable portable = new ComplexPortable(new Object());

                Data data = ss.toData(portable);
        System.out.println(ss.toObject(data));

        System.out.println(ss.getPortableContext().readField(data, "name"));
        System.out.println(ss.getPortableContext().readField(data, "age"));
//        System.out.println(ss.getPortableContext().readField(data, "namex"));

        //        System.out.println(PortableExtractor.extractValue(ss, data, "age"));
//        System.out.println(PortableExtractor.extractValue(ss, data, "name"));
//        System.out.println(PortableExtractor.extractValue(ss, data, "namex"));

//        h: 24, d: 120
//        291

    }


}

