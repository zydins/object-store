package ru.zudin.objectstore;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

/**
 * Iterator that allows to iterate over batch entities
 *
 * @author sergey
 * @since 09.05.18
 */
public interface BatchIterator extends Iterator<String>, Closeable {

    @Override
    boolean hasNext();

    /**
     * Return next key of object.
     * @return
     */
    @Override
    String next();

    default String key() {
        return next();
    }

    @Override
    void remove();

    /**
     * Return current object. Do not move pointer of iterator.
     */
    byte[] value();

    /**
     * Return current physical position of iterator
     */
    long pos();

    /**
     * Set start position of file
     */
    void setStartPos(long pos) throws IOException;

    @Override
    void close() throws IOException;

}
