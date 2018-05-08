package ru.zudin.objectstore;

import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
        store = new FileSystemObjectStore(path);
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
        store.clear();
        //todo - change
        int count = 0;
        long avgSpeed = 0;
        CircularFifoQueue<Long> last = new CircularFifoQueue<>(100);
        String str = "This is simple string, put me";
        List<String> guids = new ArrayList<>();
        for (int i = 1; i <= 10000; i++) {
            String guid = store.put(str);
            if (i % 10 == 0) {
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
            if (i > 0 && i % 100 == 0 || i < 100 && i % 10 == 0) {
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
    public void test3RemoveDefragmentation() throws Exception {
        store.clear();
        int count = 0;
        long avgSpeed = 0;
        CircularFifoQueue<Long> last = new CircularFifoQueue<>(100);
        String str = "This is simple string, put me";
        List<String> guids = new ArrayList<>();
        for (int i = 1; i <= 50000; i++) {
            String guid = store.put(str);
            if (Math.random() <= 0.4) {
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

}
