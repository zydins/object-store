package ru.zudin.objectstore.impl;

import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.*;

import static org.junit.Assert.*;

/**
 * @author Sergey Zudin
 * @since 08.05.18.
 */
public class FileSystemObjectStoreSpeedTest {

    private FileSystemObjectStore store;

    protected static String getOrCreatePath() {
        String path = System.getProperty("user.dir");
        if (!path.endsWith("/")) {
            path += "/";
        }
        path += "files/";
        File filesPath = new File(path);
        if (!filesPath.exists()) {
            filesPath.mkdir();
        }
        return path;
    }

    @Before
    public void setUp() throws Exception {
        String path = FileSystemObjectStoreSpeedTest.getOrCreatePath();
        store = new FileSystemObjectStore(path, FileSystemObjectStore.BatchType.BINARY);
    }

    @After
    public void close() throws Exception {
        store.close();
    }

    @Test
    public void test1PutSmall() throws Exception {
        store.clear();
        int count = 0;
        long avgSpeed = 0;
        CircularFifoQueue<Long> last = new CircularFifoQueue<>(100);
        String str = "This is simple string, put me";
        for (int i = 1; i <= 10000; i++) {
            long start = System.currentTimeMillis();
            store.put(str);
            long elapsed = System.currentTimeMillis() - start;
            last.add(elapsed);
            avgSpeed = (avgSpeed * count + elapsed) / ++count;
            if (i > 0 && i % 1000 == 0 || i <= 100 && i % 10 == 0) {
                long lastHun = 0;
                for (Long el : last) {
                    lastHun += el;
                }
                lastHun /= last.size();
                System.out.println(String.format("Count: %d: avg. time total=%d; avg. time of last %d=%d",
                        count, avgSpeed, last.size(), lastHun));
            }
        }
    }

    @Test
    public void test2RemoveNoDefragmentation() throws Exception {
        //todo: rewrite
        FileSystemObjectStore store = new FileSystemObjectStore(getOrCreatePath(),
                FileSystemObjectStore.BatchType.BASE_64, 4, 1, 1024 * 1024 * 500);
        try {
            store.clear();
            int count = 0;
            long avgSpeed = 0;
            CircularFifoQueue<Long> last = new CircularFifoQueue<>(100);
            String str = "This is simple string, put me";
            List<String> guids = new ArrayList<>();
            for (int i = 1; i <= 100000; i++) {
                String guid = this.store.put(str);
                if (i % 10 == 0) {
                    guids.add(guid);
                }
            }
            Collections.shuffle(guids);
            for (int i = 1; i <= guids.size(); i++) {
                String guid = guids.get(i - 1);
                long start = System.currentTimeMillis();
                this.store.delete(guid);
                long elapsed = System.currentTimeMillis() - start;
                last.add(elapsed);
                avgSpeed = (avgSpeed * count + elapsed) / ++count;
                if (i > 0 && i % 100 == 0 || i < 100 && i % 10 == 0) {
                    long lastHun = 0;
                    for (Long el : last) {
                        lastHun += el;
                    }
                    lastHun /= last.size();
                    System.out.println(String.format("Count: %d: avg. time total=%d Avg. time of last %d=%d", count, avgSpeed, last.size(), lastHun));
                }
            }
        } finally {
            store.close();
        }
    }

    @Test
    public void test3RemoveDefragmentation() throws Exception {
        store.clear();
        int count = 0;
        long avgSpeed = 0;
        CircularFifoQueue<Long> last = new CircularFifoQueue<>(100);
        String str = "This is simple string, put me";
        List<String> guids = new ArrayList<>();
        for (int i = 1; i <= 50000; i++) {
            String guid = store.put(str);
            if (Math.random() <= 0.8) {
                guids.add(guid);
            }
        }
        Collections.shuffle(guids);
        for (int i = 1; i <= guids.size(); i++) {
            String guid = guids.get(i - 1);
            long start = System.currentTimeMillis();
            store.delete(guid);
            long elapsed = System.currentTimeMillis() - start;
            last.add(elapsed);
            avgSpeed = (avgSpeed * count + elapsed) / ++count;
            if (i > 0 && i % 1000 == 0 || i < 100 && i % 10 == 0) {
                long lastHun = 0;
                for (Long el : last) {
                    lastHun += el;
                }
                lastHun /= last.size();
                System.out.println(String.format("Count: %d: avg. time total=%d Avg. time of last %d=%d", count, avgSpeed, last.size(), lastHun));
            }
        }
    }

    @Test
    public void test4PutGetRemove() throws Exception {
        store.clear();
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
    }

}
