package ru.zudin.objectstore.impl;

import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import ru.zudin.objectstore.ObjectStoreExample;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author Sergey Zudin
 * @since 08.05.18.
 */
public class FileSystemObjectStoreSpeedTest {

    private FileSystemObjectStore store;

    @Before
    public void setUp() throws Exception {
        String path = ObjectStoreExample.getOrCreatePath();
        store = new FileSystemObjectStore(path, FileSystemObjectStore.BatchType.BINARY);
    }

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

}
