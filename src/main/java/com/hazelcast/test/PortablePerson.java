package com.hazelcast.test;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

/**
* @author mdogan 20/01/15
*/
public class PortablePerson implements Portable {

    private int age;

    private long height;

    private String name;

    private PortableAddress address;

    public PortablePerson() {
    }

    public PortablePerson(int age, long height, String name, PortableAddress address) {
        this.age = age;
        this.height = height;
        this.name = name;
        this.address = address;
    }

    public int getClassId() {
        return 1;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeLong("height", height);
        writer.writeInt("age", age);
        writer.writeUTF("name", name);
        writer.writePortable("address", address);
    }

    public void readPortable(PortableReader reader) throws IOException {
        name = reader.readUTF("name");
        address = reader.readPortable("address");
        height = reader.readLong("height");
        age = reader.readInt("age");
    }

    public int getFactoryId() {
        return Test.FACTORY_ID;
    }
}
