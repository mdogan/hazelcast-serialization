package com.hazelcast.test;

import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.serialization.v1.BinaryClassDefinition;
import com.hazelcast.nio.serialization.v1.V1SerializationServiceBuilder;
import com.hazelcast.nio.serialization.v2.V2SerializationServiceBuilder;

public class Test {

    static final short FACTORY_ID = 1;

    public static void main(String[] args) throws Exception {
        PortableFactory portableFactory = new TestPortableFactory();

        SerializationService ss1 = new V1SerializationServiceBuilder()
                .addPortableFactory(FACTORY_ID, portableFactory).build();

        SerializationService ss2 = new V2SerializationServiceBuilder()
                .addPortableFactory(FACTORY_ID, portableFactory).build();

        Portable portable = new PortablePerson(123456, System.currentTimeMillis(), "some name surname",
                new PortableAddress("some street name", 987));

        print(ss1, ss2, portable);
        System.out.println();
        print(ss1, ss2, new ComplexPortable(new Object()));



//        h: 24, d: 120
//        291

    }

    private static void print(SerializationService ss1, SerializationService ss2, Portable portable) {

        Data data1 = ss1.toData(portable);
        System.out.println("h: " + data1.headerSize() + ", d: " + data1.dataSize() + ", cd: " + cdSize(ss1, data1));

        Data data2 = ss2.toData(portable);
        System.out.println("h: " + data2.headerSize() + ", d: " + data2.dataSize() + ", cd: " + cdSize(ss2, data2));
    }

    private static int cdSize(SerializationService ss1, Data data1) {
        ClassDefinition[] classDefinitions = ss1.getPortableContext().getClassDefinitions(data1);
        int k = 0;
        for (ClassDefinition classDefinition : classDefinitions) {
            k += ((BinaryClassDefinition) classDefinition).getBinary().length;
        }
        return k;
    }


}

