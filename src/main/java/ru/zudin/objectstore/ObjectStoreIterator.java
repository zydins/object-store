package ru.zudin.objectstore;

import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author sergey
 * @since 08.05.18
 */
public class ObjectStoreIterator implements Iterator {

    private static final String DIVISOR = " ";

    private final File store;
    private RandomAccessFile randomAccessFile;
    private String guid;
    private int seek;
    private String value;

    public ObjectStoreIterator(File store) {
        this.store = store;
        this.randomAccessFile = null;
        this.seek = -1;
        this.guid = null;
    }

    @Override
    public boolean hasNext() {
        init();
        try {
            return randomAccessFile.getFilePointer() + seek + 1 < randomAccessFile.length();
        } catch (IOException e) {
            throw new IllegalStateException("Cannot read file '" + store.getPath() + "");
        }
    }

    @Override
    public String next() {
        try {
            randomAccessFile.seek(randomAccessFile.getFilePointer() + seek + 1);
            String line = randomAccessFile.readLine();
            if (StringUtils.isBlank(line)) {
                throw new NoSuchElementException();
            }
            String[] split = line.split(DIVISOR);
            guid = split[0];
            seek = Integer.parseInt(split[1]);
            value = null;
            return guid;
        } catch (IOException e) {
            throw new IllegalStateException("Cannot read file: '" + store.getPath() + "");
        }
    }

    public String currentValue() {
        if (value != null) {
            return value;
        }
        try {
            value = randomAccessFile.readLine();
            return value;
        } catch (IOException e) {
            throw new IllegalStateException("Cannot read file: '" + store.getPath() + "");
        }
    }

    private void init() {
        if (randomAccessFile == null) {
            try {
                randomAccessFile = new RandomAccessFile(store, "r");
            } catch (FileNotFoundException e) {
                throw new IllegalStateException("File is not found: '" + store.getPath() + "");
            }
        }
    }


}
