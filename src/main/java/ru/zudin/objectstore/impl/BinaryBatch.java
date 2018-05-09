package ru.zudin.objectstore.impl;

import org.apache.commons.lang.BooleanUtils;
import ru.zudin.objectstore.BatchIterator;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;

/**
 * @author sergey
 * @since 09.05.18
 */
public class BinaryBatch extends AbstractFileBatch {

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
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {

    }

    class BinaryBatchIterator extends AbstractFileBatchIterator {

        @Override
        protected long nextPos(RandomAccessFile randomAccessFile, long seek) throws IOException {
            return randomAccessFile.getFilePointer() + seek;
        }

        @Override
        protected boolean readKeyAndGetStatus(RandomAccessFile randomAccessFile) throws IOException {
            boolean isActive = BooleanUtils.toBoolean(randomAccessFile.read());
            int keyLength = randomAccessFile.readInt();
            byte[] key = new byte[keyLength];
            randomAccessFile.read(key);
            this.guid = new String(key);
            this.seek = randomAccessFile.readInt();
            return isActive;
        }

        @Override
        protected void markDeleted(RandomAccessFile randomAccessFile) throws IOException {
            randomAccessFile.write(0);
        }

        @Override
        protected long updateRemovedSize(String guid, int seek) {
            return seek + String.valueOf(guid.length()).length() + guid.length() + 3 +
                    String.valueOf(seek).length(); //todo: check
        }

        @Override
        protected byte[] readValue(RandomAccessFile randomAccessFile, int seek) throws IOException {
            byte[] bytes = new byte[seek];
            randomAccessFile.read(bytes);
            return bytes;
        }

    }
}
