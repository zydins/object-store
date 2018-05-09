package ru.zudin.objectstore;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Logical representation of file batch
 *
 * @author sergey
 * @since 09.05.18
 */
public interface Batch extends Closeable {

    /**
     * Return file name
     */
    String getName();

    /**
     * Write key/value to the file.
     * @return position of key/value in file
     */
    long write(String guid, byte[] bytes) throws IOException;

    /**
     * Mark object on given position as deleted. If there is not object, nothing happens
     * @param pos position in file, returned by write(guid, bytes)
     */
    void delete(long pos) throws IOException;

    /**
     * Mark objects with given guids as deleted
     */
    void delete(Set<String> guids) throws IOException;

    /**
     * Get value that corresponds to given position. Optional.empty() if nothing found
     */
    Optional<byte[]> get(long pos) throws IOException;

    /**
     * Get size of active objects in file (in bytes)
     */
    long validSize();

    /**
     * Get total file size (in bytes)
     */
    long fileSize();

    /**
     * Defragment file if precondition is met
     */
    Optional<Map<String, Long>> defragmentIfNeeded() throws IOException;

    /**
     * Force defragmentation of file
     */
    Map<String, Long> defragment() throws IOException;

    /**
     * Create iterator over file entries
     */
    BatchIterator createIterator() throws IOException;
}
