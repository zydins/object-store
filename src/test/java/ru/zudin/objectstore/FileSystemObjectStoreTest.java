package ru.zudin.objectstore;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.*;

/**
 * @author sergey
 * @since 07.05.18
 */
public class FileSystemObjectStoreTest {

    private FileSystemObjectStore store;

    @Before
    public void setUp() throws Exception {
        store = new FileSystemObjectStore("/Users/sergey/zydins/object-store/files/"); //temp
    }

    @Test
    public void testPut1() throws Exception {
        String value = "Test string 1";
        String guid = store.put(value);
        Optional<Object> optional = store.get(guid);
        assertTrue(optional.isPresent());
        Object obj = optional.get();
        assertTrue(obj instanceof String);
        assertEquals(value, obj);
    }

    @Test
    public void testPut2() throws Exception {
        String value = "Test string 2";
        store.put(value); //put in the same bucket
        String guid = store.put(value);
        Optional<Object> optional = store.get(guid);
        assertTrue(optional.isPresent());
        Object obj = optional.get();
        assertTrue(obj instanceof String);
        assertEquals(value, obj);
    }

    @Test
    public void testPut3() throws Exception {
        HashMap<Integer, String> map = new HashMap<>();
        for (int i = 0; i < 10000; i++) {
            map.put(i, "This is my string, hello " + i);
        }
        String guid = store.put(map);
        Optional<Object> optional = store.get(guid);
        assertTrue(optional.isPresent());
        Object obj = optional.get();
        assertTrue(obj instanceof Map);
        Map<Integer, String> saved = (Map<Integer, String>) obj;
        assertEquals(map.size(), saved.size());
        for (Integer key : map.keySet()) {
            String s = saved.get(key);
            assertNotNull(s);
            assertEquals(map.get(key), s);
        }
    }

}