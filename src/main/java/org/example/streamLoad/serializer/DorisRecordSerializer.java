package org.example.streamLoad.serializer;

import java.io.IOException;
import java.io.Serializable;

/**
 * How to serialize the record to bytes.
 * @param <T>
 */
public interface DorisRecordSerializer<T> extends Serializable {

    /**
     * define how to convert record into byte array.
     * @param record
     * @return byte array
     * @throws IOException
     */
    byte[] serialize(T record) throws IOException;
}
