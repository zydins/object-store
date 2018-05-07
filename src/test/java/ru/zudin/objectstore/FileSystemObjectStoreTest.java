package ru.zudin.objectstore;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
        Assert.assertTrue(optional.isPresent());
        Object obj = optional.get();
        Assert.assertTrue(obj instanceof String);
        Assert.assertEquals(value, obj);
    }

    @Test
    public void testPut2() throws Exception {
        String value = "Test string 2";
        store.put(value); //put in the same bucket
        String guid = store.put(value);
        Optional<Object> optional = store.get(guid);
        Assert.assertTrue(optional.isPresent());
        Object obj = optional.get();
        Assert.assertTrue(obj instanceof String);
        Assert.assertEquals(value, obj);
    }

}