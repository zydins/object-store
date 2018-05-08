package ru.zudin.objectstore;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Optional;

/**
 * @author sergey
 * @since 07.05.18
 */
public interface AppendOnlyObjectStore {

    String put(Serializable object) throws FileNotFoundException, IOException;

    Optional<Object> get(String guid) throws FileNotFoundException, IOException;

    void delete(String guid);

    void delete(Collection<String> guids);
}
