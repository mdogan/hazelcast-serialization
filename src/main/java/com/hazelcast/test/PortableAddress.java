package com.hazelcast.test;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

/**
* @author mdogan 20/01/15
*/
public class PortableAddress implements Portable {

    private String street ;

    private int no ;

    public PortableAddress() {
    }

    public PortableAddress(String street, int no) {
        this.street = street;
        this.no = no;
    }

    public int getClassId() {
        return 2;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeInt("no", no);
        writer.writeUTF("street", street);
    }

    public void readPortable(PortableReader reader) throws IOException {
        street = reader.readUTF("street");
        no = reader.readInt("no");
    }

    public int getFactoryId() {
        return Test.FACTORY_ID;
    }
}
