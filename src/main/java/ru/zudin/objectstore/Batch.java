package ru.zudin.objectstore;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * @author sergey
 * @since 09.05.18
 */
public interface Batch extends Closeable {

    String getName();

    long write(String guid, byte[] bytes) throws IOException;

    void delete(long pos) throws IOException;

    void delete(Set<String> guids);

    Optional<byte[]> get(long pos) throws IOException;

    long validSize();

    Optional<Map<String, Long>> defragmentIfNeeded() throws IOException;

    BatchIterator createIterator();

    long fileSize();

    Map<String, Long> defragment() throws IOException;
}
