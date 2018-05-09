package ru.zudin.objectstore.impl;

import org.apache.commons.lang.BooleanUtils;
import ru.zudin.objectstore.BatchIterator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

/**
 * @author sergey
 * @since 09.05.18
 */
public class BinaryBatch extends AbstractFileBatch {

    public BinaryBatch(String path, String name, double sizeLoadFactor, long fileSizeThreshold) {
        super(path, name, sizeLoadFactor, fileSizeThreshold);
    }

    @Override
    public long write(String guid, byte[] bytes) throws IOException {
        RandomAccessFile accessFile = new RandomAccessFile(file, "rw");
        long pos = file.length();
        accessFile.seek(pos);
        write(accessFile, guid, bytes);
        accessFile.close();
        return pos;
    }

    private void write(RandomAccessFile accessFile, String guid, byte[] bytes) throws IOException {
        int keyLength = guid.length();
        int valueLength = bytes.length;
        accessFile.writeByte(1);
        accessFile.writeInt(keyLength);
        accessFile.writeBytes(guid);
        accessFile.writeInt(valueLength);
        accessFile.write(bytes);
    }

    @Override
    protected BatchIterator innerCreateIterator() {
        return new BinaryBatchIterator();
    }

    @Override
    protected Map<String, Long> innerDefragment() throws FileNotFoundException, IOException {
        Map<String, Long> positions = new HashMap<>();
        File newFile = new File(file.getName() + ".new");
        RandomAccessFile newFileWriter = new RandomAccessFile(newFile, "rw");
        BatchIterator oldIterator = createIterator();
        while (oldIterator.hasNext()) {
            long pos = newFileWriter.getFilePointer();
            String guid = oldIterator.next();
            byte[] bytes = oldIterator.value();
            write(newFileWriter, guid, bytes);
            positions.put(guid, pos);
        }
        newFileWriter.close();
        File tempOld = new File(file.getPath() + ".old");
        file.renameTo(tempOld);
        newFile.renameTo(file);
        tempOld.delete();
        return positions;
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
