package com.hazelcast.test;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;

/**
* @author mdogan 20/01/15
*/
class TestPortableFactory implements PortableFactory {
    public Portable create(int classId) {
        switch (classId) {
            case 1:
                return new PortablePerson();
            case 2:
                return new PortableAddress();
            case 3:
                return new ComplexPortable();
        }
        throw new IllegalArgumentException();
    }
}
