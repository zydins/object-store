package ru.zudin.objectstore;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Optional;

/**
 * Append-only store for serializable objects.
 *
 * @author sergey
 * @since 07.05.18
 */
public interface ObjectStore {

    /**
     * Inserts a single serializable object to store.
     *
     * @param object serializable object
     * @return identifier (guid) of this object in store
     * @throws IOException if an I/O error occurs.
     */
    String put(Serializable object) throws IOException;

    /**
     * Retrieves element from the store for given guid.
     *
     * @param guid identifier of the object in store from "put" method
     * @return empty Optional if object is not found or Optional with stored value
     * @throws IOException if an I/O error occurs.
     */
    Optional<Object> get(String guid) throws IOException;

    /**
     * Deletes a single object from store by its guid.
     * If guid is not found in store, nothing happens
     *
     * @param guid identifier of the object in store from "put" method
     * @throws IOException if an I/O error occurs.
     */
    void delete(String guid) throws IOException;

    /**
     * Deletes a multiple objects from store.
     * It should be more efficient for multiple objects because of less I/O operations.
     *
     * @param guids collections of objects identifiers
     * @throws IOException if an I/O error occurs.
     */
    void delete(Collection<String> guids) throws IOException;
}
