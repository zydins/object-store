package ru.zudin.objectstore;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.*;

import static org.junit.Assert.*;

/**
 * @author sergey
 * @since 07.05.18
 */
public class FileSystemObjectStoreTest {

    private FileSystemObjectStore store;

    @Before
    public void setUp() throws Exception {
        String path = System.getProperty("user.dir");
        if (!path.endsWith("/")) {
            path += "/";
        }
        path += "files/";
        File filesPath = new File(path);
        if (!filesPath.exists()) {
            filesPath.mkdir();
        }
        store = new FileSystemObjectStore(path); //temp
    }

    @Test
    public void test1PutGet() throws Exception {
        store.clear();
        String value = "Test string 1";
        String guid = store.put(value);
        Optional<Object> optional = store.get(guid);
        assertTrue(optional.isPresent());
        Object obj = optional.get();
        assertTrue(obj instanceof String);
        assertEquals(value, obj);
    }

    @Test
    public void test2PutsGet() throws Exception {
        store.clear();
        Map<String, String> guids = new HashMap<>();
        for (int i = 0; i < 20; i++) {
            String value = "Hello wonkies " + i;
            String guid = store.put(value);
            guids.put(guid, value);
        }
        for (String guid : guids.keySet()) {
            Optional<Object> optional = store.get(guid);
            assertTrue(optional.isPresent());
            Object obj = optional.get();
            assertTrue(obj instanceof String);
            assertEquals(guids.get(guid), obj);
        }
    }

    @Test
    public void test3BigObject() throws Exception {
        store.clear();
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

    @Test
    public void test4Remove() throws Exception {
        store.clear();
        List<String> removed = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            String value = "Hello wonkies " + i;
            String guid = store.put(value);
            if (i % 4 == 0) {
                removed.add(guid);
            }
        }
        for (String guid : removed) {
            store.delete(guid);
        }
        for (String guid : removed) {
            Optional<Object> optional = store.get(guid);
            assertTrue(!optional.isPresent());
        }
    }

    @Test
    public void test5RemoveGet() throws Exception {
        store.clear();
        Multimap<Boolean, String> states = HashMultimap.create();
        for (int i = 0; i < 20; i++) {
            String value = "Hello wonkies " + i;
            String guid = store.put(value);
            if (i % 4 == 0) {
                states.put(true, guid);
            } else {
                states.put(false, guid);
            }
        }
        for (String guid : states.get(true)) {
            store.delete(guid);
        }
        for (String guid : states.get(true)) {
            Optional<Object> optional = store.get(guid);
            assertTrue(!optional.isPresent());
        }
        for (String guid : states.get(false)) {
            Optional<Object> optional = store.get(guid);
            assertTrue(optional.isPresent());
        }
    }

}