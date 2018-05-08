package ru.zudin.objectstore;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
        String path = FileSystemObjectStoreSpeedTest.getOrCreatePath();
        store = new FileSystemObjectStore(path);
    }

    @After
    public void close() throws Exception {
        store.close();
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

    @Test
    public void test6RemoveMultiple() throws Exception {
        store.clear();
        List<String> removed = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            String value = "Hello wonkies " + i;
            String guid = store.put(value);
            if (i % 4 == 0) {
                removed.add(guid);
            }
        }
        store.delete(removed);
        for (String guid : removed) {
            Optional<Object> optional = store.get(guid);
            assertTrue(!optional.isPresent());
        }
    }

    @Test
    public void test7Defragment() throws Exception {
        store.clear();
        List<Batch> batches = store.getBatches();
        System.out.println("### Before insert ###");
        for (Batch batch : batches) {
            System.out.println(String.format("Batch %s, size: %d", batch.getName(), batch.fileSize()));
        }
        List<String> toDelete = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            String value = "Hello wonkies " + i;
            String guid = store.put(value);
            if (i % 4 == 0) {
                toDelete.add(guid);
            }
        }
        System.out.println("### After insert ###");
        for (Batch batch : batches) {
            System.out.println(String.format("Batch %s, size: %d", batch.getName(), batch.fileSize()));
        }
        store.delete(toDelete);
        System.out.println("### After delete ###");
        for (Batch batch : batches) {
            long oldSize = batch.fileSize();
            batch.defragment();
            long newSize = batch.fileSize();
            System.out.println(String.format("Batch %s, size after logical delete: %d, size after file delete: %d",
                    batch.getName(), oldSize, newSize));
            assertTrue(newSize < oldSize);
        }
    }

}