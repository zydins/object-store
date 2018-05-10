package ru.zudin.objectstore;

import ru.zudin.objectstore.impl.FileSystemObjectStore;

import java.io.File;
import java.util.*;

import static org.junit.Assert.*;

/**
 * @author sergey
 * @since 09.05.18
 */
public class ObjectStoreExample {

    public static void main(String... args) throws Exception {
        FileSystemObjectStore store = new FileSystemObjectStore(getOrCreatePath(), FileSystemObjectStore.BatchType.BINARY);
        try {
            store.deleteFiles();
            test(store);
        } finally {
            store.close();
        }
    }

    public static String getOrCreatePath() {
        String path = System.getProperty("user.dir");
        if (!path.endsWith(File.separator)) {
            path += File.separator;
        }
        path += "files" + File.separator;
        File filesPath = new File(path);
        if (!filesPath.exists()) {
            filesPath.mkdir();
        }
        return path;
    }

    public static void test(ObjectStore store) throws Exception {
        int count = 0;
        long timeTotal = 0;
        String str = "Test string ";
        List<String> guids = new ArrayList<>();
        System.out.println("### Insert ###");
        for (int i = 1; i <= 100000; i++) {
            int random = new Random().nextInt((int) Math.sqrt(i));
            ArrayList<String> list = new ArrayList<>();
            for (int j = 0; j <= random; j++) {
                list.add(str + i + "/" + j);
            }
            HashMap<Integer, List<String>> map = new HashMap<>();
            map.put(list.hashCode(), list);
            long start = System.currentTimeMillis();
            String guid = store.put(map);
            long elapsed = System.currentTimeMillis() - start;
            timeTotal += elapsed;
            count++;
            guids.add(guid);
            if (i % 5000 == 0) {
                System.out.println(String.format("Count: %d: avg. time=%f, total=%d", count,
                        timeTotal / (double) count, timeTotal));
            }
        }

        System.out.println("### Get 1 ###");
        Collections.shuffle(guids);
        count = 0;
        timeTotal = 0;
        for (int i = 1; i <= 10000; i++) {
            String guid = guids.get(i - 1);
            long start = System.currentTimeMillis();
            Optional<Object> optional = store.get(guid);
            long elapsed = System.currentTimeMillis() - start;
            timeTotal += elapsed;
            count++;
            if (i % 1000 == 0) {
                System.out.println(String.format("Count: %d: avg. time=%f, total=%d", count,
                        timeTotal / (double) count, timeTotal));
            }
            assertTrue(optional.isPresent());
            Object o = optional.get();
            assertTrue(o instanceof Map);
            Map<Integer, List<String>> map = (Map<Integer, List<String>>) o;
            for (Integer hash : map.keySet()) {
                List<String> list = map.get(hash);
                assertEquals(hash.intValue(), list.hashCode());
            }
        }

        System.out.println("### Delete ###");
        Collections.shuffle(guids);
        count = 0;
        timeTotal = 0;
        Iterator<String> iterator = guids.iterator();
        int ind = 0;
        while (iterator.hasNext() && ind++ < 50000) {
            String guid = iterator.next();
            long start = System.currentTimeMillis();
            store.delete(guid);
            long elapsed = System.currentTimeMillis() - start;
            timeTotal += elapsed;
            count++;
            if (ind % 5000 == 0) {
                System.out.println(String.format("Count: %d: avg. time=%f, total=%d", count,
                        timeTotal / (double) count, timeTotal));
            }
            Optional<Object> optional = store.get(guid);
            assertFalse(optional.isPresent());
            iterator.remove();
        }

        System.out.println("### Get 2 ###");
        Collections.shuffle(guids);
        count = 0;
        timeTotal = 0;
        for (int i = 1; i <= guids.size(); i++) {
            String guid = guids.get(i - 1);
            long start = System.currentTimeMillis();
            Optional<Object> optional = store.get(guid);
            long elapsed = System.currentTimeMillis() - start;
            timeTotal += elapsed;
            count++;
            if (i % 5000 == 0) {
                System.out.println(String.format("Count: %d: avg. time=%f, total=%d", count,
                        timeTotal / (double) count, timeTotal));
            }
            assertTrue(optional.isPresent());
            Object o = optional.get();
            assertTrue(o instanceof Map);
            Map<Integer, List<String>> map = (Map<Integer, List<String>>) o;
            for (Integer hash : map.keySet()) {
                List<String> list = map.get(hash);
                assertEquals(hash.intValue(), list.hashCode());
            }
        }
        System.out.println("Everything good");
    }
}
