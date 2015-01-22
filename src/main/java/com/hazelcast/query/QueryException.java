package com.hazelcast.query;


import com.hazelcast.core.HazelcastException;

/**
 * @author mdogan 20/01/15
 */
public class QueryException extends HazelcastException {

    public QueryException(String s) {
        super(s);
    }
}
