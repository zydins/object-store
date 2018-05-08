package ru.zudin.objectstore;

import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

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
        for (int i = 0; i < 10000; i++) {
            long start = System.currentTimeMillis();
            store.put(str);
            long elapsed = System.currentTimeMillis() - start;
            last.add(elapsed);
            avgSpeed = (avgSpeed * count + elapsed) / ++count;
            if (i > 0 && i % 100 == 0 || i < 100 && i % 10 == 0) {
                System.out.println("Count: "+count+", Avg. time: " + avgSpeed);
                long lastHun = 0;
                for (Long el : last) {
                    lastHun += el;
                }
                lastHun /= last.size();
                System.out.println("Count: "+count+", Avg. time of last "+last.size()+": " + lastHun);
            }
        }
    }

}
