package ru.zudin.objectstore;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;

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
    private long pos;
    private String value;
    private Optional<Boolean> hasNext;

    public ObjectStoreIterator(File store) {
        this.store = store;
        this.randomAccessFile = null;
        this.seek = -1;
        this.guid = null;
        this.hasNext = Optional.empty();
    }

    @Override
    public boolean hasNext() {
        init();
        try {
            if (hasNext.isPresent()) {
                return hasNext.get();
            }
            boolean iterate = iterate();
            hasNext = Optional.of(iterate);
            return iterate;
        } catch (IOException e) {
            throw new IllegalStateException("Cannot read file '" + store.getPath() + "");
        }
    }

    @Override
    public String next() {
        init();
        try {
            if (hasNext.isPresent()) {
                if (!hasNext.get()) {
                    throw new NoSuchElementException();
                }
                hasNext = Optional.empty();
            } else {
                if (!iterate()) {
                    throw new NoSuchElementException();
                }
            }
            value = null;
            return guid;
        } catch (IOException e) {
            throw new IllegalStateException("Cannot read file: '" + store.getPath() + "");
        }
    }

    private boolean iterate() throws IOException {
        while (true) {
            pos = nextLine();
            randomAccessFile.seek(pos);
            String line = randomAccessFile.readLine();
            if (StringUtils.isBlank(line)) {
                return false;
            }
            String[] split = line.split(DIVISOR);
            guid = split[1];
            seek = Integer.parseInt(split[2]);
            boolean isActive = BooleanUtils.toBoolean(Integer.parseInt(split[0]));
            if (!isActive) {
                continue;
            }
            return true;
        }
    }

    private long nextLine() throws IOException {
        return nextLine(0);
    }

    private long nextLine(int shift) throws IOException {
        return randomAccessFile.getFilePointer() + seek + 1 - shift;
    }

    @Override
    public void remove() {
        try {
            randomAccessFile.seek(pos);
            randomAccessFile.writeBytes("0"); //mark removed
            randomAccessFile.seek(nextLine(1));
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
                randomAccessFile = new RandomAccessFile(store, "rw");
            } catch (FileNotFoundException e) {
                throw new IllegalStateException("File is not found: '" + store.getPath() + "");
            }
        }
    }


}
