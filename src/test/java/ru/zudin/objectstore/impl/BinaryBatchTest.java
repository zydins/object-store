package ru.zudin.objectstore.impl;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import ru.zudin.objectstore.BatchIterator;
import ru.zudin.objectstore.ObjectStoreExample;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Optional;

import static org.junit.Assert.*;

/**
 * @author sergey
 * @since 09.05.18
 */
public class BinaryBatchTest {

    private BinaryBatch batch;

    @Before
    public void setUp() throws Exception {
        String path = ObjectStoreExample.getOrCreatePath();
        batch = new BinaryBatch(path, "test-batch.tmp", 0.33, 1024 * 1024 * 200);
    }

    @After
    public void close() throws Exception {
        batch.close();
    }

    @Test
    public void test1WriteGet() throws Exception {
        if (batch.file.exists()) {
            batch.file.delete();
            batch.file.createNewFile();
        }
        String key = "12345";
        byte[] bytes = new byte[5];
        bytes[2] = 20;
        long pos = batch.write(key, bytes);
        Optional<byte[]> optional = batch.get(pos);
        assertTrue(optional.isPresent());
        byte[] saved = optional.get();
        assertEquals(bytes.length, saved.length);
        for (int i = 0; i < bytes.length; i++) {
            assertEquals(bytes[i], saved[i]);
        }
    }

    @Test
    public void test2Iterator() throws Exception {
        if (batch.file.exists()) {
            batch.file.delete();
            batch.file.createNewFile();
        }
        String key = "12345";
        byte[] bytes = new byte[5];
        bytes[2] = 20;
        long pos = batch.write(key, bytes);
        BatchIterator iterator = null;
        try {
            iterator = batch.createIterator();
            assertTrue(iterator.hasNext());
            String next = iterator.next();
            assertEquals(key, next);
            byte[] saved = iterator.value();
            assertNotNull(saved);
            assertEquals(bytes.length, saved.length);
            for (int i = 0; i < bytes.length; i++) {
                assertEquals(bytes[i], saved[i]);
            }
            assertFalse(iterator.hasNext());
        } finally {
            if (iterator != null) {
                iterator.close();
            }
        }
    }

    @Test
    public void test3EmptyIterator() throws Exception {
        batch.file.delete();
        try {
            batch.createIterator();
            assertTrue(false);
        } catch (IOException e) {
            assertTrue(true);
        } finally {
            batch.file.createNewFile();
        }
    }

    @Test
    public void test4GetValueAgain() throws Exception {
        if (batch.file.exists()) {
            batch.file.delete();
            batch.file.createNewFile();
        }
        String key = "12345";
        byte[] bytes = new byte[5];
        bytes[2] = 20;
        long pos = batch.write(key, bytes);
        BatchIterator iterator = null;
        try {
            iterator = batch.createIterator();
            assertTrue(iterator.hasNext());
            String next = iterator.next();
            assertEquals(key, next);
            byte[] saved = iterator.value();
            assertNotNull(saved);
            byte[] saved2 = iterator.value();
            assertNotNull(saved2);
            assertEquals(saved, saved2);
        } finally {
            if (iterator != null) {
                iterator.close();
            }
        }
    }

    @Test
    public void test5Remove() throws Exception {
        if (batch.file.exists()) {
            batch.file.delete();
            batch.file.createNewFile();
        }
        String key = "12345";
        byte[] bytes = new byte[5];
        bytes[2] = 20;
        long pos = batch.write(key, bytes);
        BatchIterator iterator = null;
        try {
            iterator = batch.createIterator();
            iterator.next();
            iterator.remove();
            iterator.close();
            iterator = batch.createIterator();
            assertFalse(iterator.hasNext());
            Optional<byte[]> optional = batch.get(pos);
            assertFalse(optional.isPresent());
        } finally {
            if (iterator != null) {
                iterator.close();
            }
        }
    }

    @Test
    public void test6WrongRemove() throws Exception {
        if (batch.file.exists()) {
            batch.file.delete();
            batch.file.createNewFile();
        }
        String key = "12345";
        byte[] bytes = new byte[5];
        bytes[2] = 20;
        long pos = batch.write(key, bytes);
        BatchIterator iterator = null;
        try {
            iterator = batch.createIterator();
            iterator.remove();
            assertTrue(false);
        } catch (IllegalStateException e) {
            assertTrue(true);
        } finally {
            if (iterator != null) {
                iterator.close();
            }
        }
    }

    @Test
    public void test7WrongRemove() throws Exception {
        if (batch.file.exists()) {
            batch.file.delete();
            batch.file.createNewFile();
        }
        String key = "12345";
        byte[] bytes = new byte[5];
        bytes[2] = 20;
        long pos = batch.write(key, bytes);
        BatchIterator iterator = null;
        try {
            iterator = batch.createIterator();
            iterator.hasNext();
            iterator.remove();
            assertTrue(false);
        } catch (IllegalStateException e) {
            assertTrue(true);
        } finally {
            if (iterator != null) {
                iterator.close();
            }
        }
    }

    @Test
    public void test8Pos() throws Exception {
        if (batch.file.exists()) {
            batch.file.delete();
            batch.file.createNewFile();
        }
        String key = "12345";
        byte[] bytes = new byte[5];
        bytes[2] = 20;
        long pos = batch.write(key, bytes);
        batch.write(key, bytes);
        BatchIterator iterator = null;
        try {
            iterator = batch.createIterator();
            iterator.next();
            long l = iterator.pos();
            assertTrue(l >= 0);
            iterator.next();
            long newPos = iterator.pos();
            assertTrue(newPos > l);
        } catch (IllegalStateException e) {
            assertTrue(true);
        } finally {
            if (iterator != null) {
                iterator.close();
            }
        }
    }

    @Test
    public void test9Nextfail() throws Exception {
        if (batch.file.exists()) {
            batch.file.delete();
            batch.file.createNewFile();
        }
        BatchIterator iterator = null;
        try {
            iterator = batch.createIterator();
            iterator.next();
            assertTrue(false);
        } catch (NoSuchElementException e) {
            assertTrue(true);
        } finally {
            if (iterator != null) {
                iterator.close();
            }
        }
    }

    @Test
    public void test10Nextfail() throws Exception {
        if (batch.file.exists()) {
            batch.file.delete();
            batch.file.createNewFile();
        }
        BatchIterator iterator = null;
        try {
            iterator = batch.createIterator();
            assertFalse(iterator.hasNext());
            iterator.next();
            assertTrue(false);
        } catch (NoSuchElementException e) {
            assertTrue(true);
        } finally {
            if (iterator != null) {
                iterator.close();
            }
        }
    }

}