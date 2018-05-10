package ru.zudin.objectstore.impl;

import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import ru.zudin.objectstore.ObjectStoreExample;

import java.util.*;

/**
 * @author Sergey Zudin
 * @since 08.05.18.
 */
public abstract class AbstractFSOSSpeedTest {

    private FileSystemObjectStore store;

    @Before
    public void setUp() throws Exception {
        String path = ObjectStoreExample.getOrCreatePath();
        store = new FileSystemObjectStore(path, getType());
    }

    protected abstract FileSystemObjectStore.BatchType getType();

    @After
    public void close() throws Exception {
        store.close();
    }

    @Test
    public void test1PutSmall() throws Exception {
        store.deleteFiles();
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
        FileSystemObjectStore store = new FileSystemObjectStore(ObjectStoreExample.getOrCreatePath(),
                FileSystemObjectStore.BatchType.BASE_64, 4, 1, 1024 * 1024 * 500);
        try {
            store.deleteFiles();
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
        store.deleteFiles();
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
        store.deleteFiles();
        ObjectStoreExample.test(store);
    }

    @Test
    public void test5Workflow() throws Exception {
        store.deleteFiles();
        Random random = new Random();
        Map<String, String> guids = new HashMap<>();
        String str = "Lucy in the sky with %d diamonds";
        String format = getFormat(str, 0);
        guids.put(store.put(format), format);
        int inserted = 0;
        int deleted = 0;
        int gets = 0;
        long start = System.currentTimeMillis();
        for (int i = 1; i < 100000; i++) {
            int nextInt = random.nextInt(100);
            if (nextInt % 10 == 0) {
                format = getFormat(str, i);
                String guid = store.put(format);
                guids.put(guid, format);
                inserted++;
            } else if (nextInt == 55) {
                String guid = getRandomGuid(guids, random);
                store.delete(guid);
                Optional<Object> optional = store.get(guid);
                Assert.assertFalse(optional.isPresent());
                guids.remove(guid);
                deleted++;
            } else if (!guids.isEmpty()) {
                String guid = getRandomGuid(guids, random);
                Optional<Object> optional = store.get(guid);
                Assert.assertTrue(optional.isPresent());
                Assert.assertEquals(guids.get(guid), optional.get());
                gets++;
            }
            if (i % 1000 == 0) {
                System.out.println(String.format("%d: Inserted=%d, Deleted=%d, Gets=%d", i, inserted, deleted, gets));
            }
        }
        long elapsed = System.currentTimeMillis() - start;
        System.out.println("All good, took " + elapsed);
    }

    private String getRandomGuid(Map<String, String> guids, Random random) {
        Set<String> strings = guids.keySet();
        return strings.stream()
                .skip(random.nextInt(strings.size()))
                .findFirst()
                .get();
    }

    private String getFormat(String str, int i) {
        return String.format(str, i);
    }

}
