package com.hazelcast.test;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;
import java.util.Random;

/**
 * @author mdogan 21/01/15
 */
public class ComplexPortable implements Portable {

    private short short1;
    private short short22;
    private short short333;

    private int int1;
    private int int22;
    private int int333;

    private long long1;
    private long long22;
    private long long333;

    private double double1;
    private double double22;
    private double double333;

    private String aString;
    private String bString;
    private String cString;

    private boolean bool1;
    private boolean bool22;
    private boolean bool333;

    public ComplexPortable() {
    }

    public ComplexPortable(Object dummy) {
        Random rand = new Random();

        bool1 = rand.nextBoolean();
        bool22 = rand.nextBoolean();
        bool333 = rand.nextBoolean();

        short1 = (short) rand.nextInt();
        short22 = (short) rand.nextInt();
        short333 = (short) rand.nextInt();

        int1 = rand.nextInt();
        int22 = rand.nextInt();
        int333 = rand.nextInt();

        long1 = rand.nextLong();
        long22 = rand.nextLong();
        long333 = rand.nextLong();

        double1 = rand.nextDouble();
        double22 = rand.nextDouble();
        double333 = rand.nextDouble();

        aString = createStr(rand);
        bString = createStr(rand);
        cString = createStr(rand);
    }

    private String createStr(Random rand) {
        int k = rand.nextInt(96) + 4;
        byte[] bytes = new byte[k];
        rand.nextBytes(bytes);
        return new String(bytes);
    }

    @Override
    public int getFactoryId() {
        return 1;
    }

    @Override
    public int getClassId() {
        return 3;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeBoolean("bool1", bool1);
        writer.writeShort("short1", short1);
        writer.writeInt("int1", int1);
        writer.writeLong("long1", long1);
        writer.writeDouble("double1", double1);
        writer.writeUTF("aString", aString);

        writer.writeBoolean("bool22", bool22);
        writer.writeShort("short22", short22);
        writer.writeInt("int22", int22);
        writer.writeLong("long22", long22);
        writer.writeDouble("double22", double22);
        writer.writeUTF("bString", bString);

        writer.writeBoolean("bool333", bool333);
        writer.writeShort("short333", short333);
        writer.writeInt("int333", int333);
        writer.writeLong("long333", long333);
        writer.writeDouble("double333", double333);
        writer.writeUTF("cString", cString);
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        bool1 = reader.readBoolean("bool1");
        bool22 = reader.readBoolean("bool22");
        bool333 = reader.readBoolean("bool333");

        short1 = reader.readShort("short1");
        short22 = reader.readShort("short22");
        short333 = reader.readShort("short333");

        int1 = reader.readInt("int1");
        int22 = reader.readInt("int22");
        int333 = reader.readInt("int333");

        long1 = reader.readLong("long1");
        long22 = reader.readLong("long22");
        long333 = reader.readLong("long333");

        double1 = reader.readDouble("double1");
        double22 = reader.readDouble("double22");
        double333 = reader.readDouble("double333");

        aString = reader.readUTF("aString");
        bString = reader.readUTF("bString");
        cString = reader.readUTF("cString");
    }

}
