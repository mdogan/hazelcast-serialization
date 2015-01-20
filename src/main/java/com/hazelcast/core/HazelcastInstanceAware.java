package com.hazelcast.core;

/**
 * @author mdogan 20/01/15
 */
public interface HazelcastInstanceAware {

    void setHazelcastInstance(HazelcastInstance hazelcastInstance);
}
