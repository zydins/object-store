package ru.zudin.objectstore.impl;

import org.apache.commons.lang.BooleanUtils;
import ru.zudin.objectstore.BatchIterator;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * @author sergey
 * @since 09.05.18
 */
public class BinaryBatch extends AbstractBatch {

    private long removedSize;

    public BinaryBatch(String path, String name, double sizeLoadFactor, long fileSizeThreshold) {
        super(path, name, sizeLoadFactor, fileSizeThreshold);
    }

    @Override
    public long write(String guid, byte[] bytes) throws IOException {
        long pos = file.length();
        int keyLength = guid.length();
        int valueLength = bytes.length;
        RandomAccessFile accessFile = new RandomAccessFile(file, "rw");
        accessFile.seek(file.length());
        accessFile.writeByte(1);
        accessFile.writeInt(keyLength);
        accessFile.writeBytes(guid);
        accessFile.writeInt(valueLength);
        accessFile.write(bytes);
        accessFile.close();
        return pos;
    }

    @Override
    public long validSize() {
        return fileSize() - removedSize;
    }

    @Override
    public BatchIterator createIterator() {
        return new BinaryBatchIterator();
    }

    @Override
    public Map<String, Long> defragment() throws IOException {
        return null;
    }

    @Override
    public void close() throws IOException {

    }

    class BinaryBatchIterator implements BatchIterator {

        private RandomAccessFile randomAccessFile;
        private String guid;
        private int valueLength;
        private long pos;
        private byte[] value;
        private Optional<Boolean> hasNext;

        public BinaryBatchIterator() {
            this.randomAccessFile = null;
            this.valueLength = -1;
            this.guid = null;
            this.hasNext = Optional.empty();
        }

        private void init() {
            if (randomAccessFile == null) {
                try {
                    randomAccessFile = new RandomAccessFile(file, "rw");
                } catch (FileNotFoundException e) {
                    throw new IllegalStateException("File is not found: '" + file.getPath() + "");
                }
            }
        }

        private boolean iterate() throws IOException {
            while (true) {
                pos = randomAccessFile.getFilePointer() + valueLength + 1;
                if (randomAccessFile.length() <= pos) {
                    return false;
                }
                randomAccessFile.seek(pos);
                boolean isActive = BooleanUtils.toBoolean(randomAccessFile.read());
                int keyLength = randomAccessFile.readInt();
                byte[] key = new byte[keyLength];
                randomAccessFile.read(key);
                this.guid = new String(key);
                this.valueLength = randomAccessFile.readInt();
                if (!isActive) {
                    continue;
                }
                return true;
            }
        }

        @Override
        public boolean hasNext() {
            init();
            try {
                if (hasNext.isPresent()) {
                    return hasNext.get();
                }
                boolean iterate = iterate();
                this.hasNext = Optional.of(iterate);
                return iterate;
            } catch (IOException e) {
                throw new IllegalStateException("Cannot read file '" + file.getPath() + "");
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
                throw new IllegalStateException("Cannot read file: '" + file.getPath() + "");
            }
        }

        @Override
        public void remove() {
            try {
                long prev = randomAccessFile.getFilePointer();
                randomAccessFile.seek(pos);
                randomAccessFile.write(0);
                randomAccessFile.seek(prev);
                removedSize += valueLength + String.valueOf(guid.length()).length() + guid.length() + 3 +
                        String.valueOf(valueLength).length(); //todo: check
            } catch (IOException e) {
                throw new IllegalStateException("Cannot read file: '" + file.getPath() + "");
            }
        }

        @Override
        public byte[] value() {
            if (value != null) {
                return value;
            }
            try {
                long prev = randomAccessFile.getFilePointer();
                value = new byte[valueLength];
                randomAccessFile.read(value);
                randomAccessFile.seek(prev);
                return value;
            } catch (IOException e) {
                throw new IllegalStateException("Cannot read file: '" + file.getPath() + "");
            }
        }

        @Override
        public long pos() {
            return pos;
        }

        @Override
        public void setStartPos(long pos) throws IOException {
            init();
            randomAccessFile.seek(pos);
        }

        @Override
        public void close() throws IOException {
            randomAccessFile.close();
        }
    }
}
