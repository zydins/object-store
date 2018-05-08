package ru.zudin.objectstore;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

/**
 * @author sergey
 * @since 09.05.18
 */
public interface BatchIterator extends Iterator<String>, Closeable {

    @Override
    boolean hasNext();

    @Override
    String next();

    default String key() {
        return next();
    }

    @Override
    void remove();

    byte[] value();

    long pos();

    void setStartPos(long pos) throws IOException;

    @Override
    void close() throws IOException;

}
