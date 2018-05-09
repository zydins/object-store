package ru.zudin.objectstore.impl;

import org.apache.commons.lang.BooleanUtils;
import ru.zudin.objectstore.BatchIterator;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of file batch that stores objects as binary array.
 *
 * Entry in this case contains of:
 * - state
 * - guid size
 * - guid
 * - object size
 * - object itself
 *
 * These elements are stored one by one in binary file.
 *
 * @author sergey
 * @since 09.05.18
 */
public class BinaryBatch extends AbstractFileBatch {

    public BinaryBatch(String path, String name, double sizeLoadFactor, long fileSizeThreshold) {
        super(path, name, sizeLoadFactor, fileSizeThreshold);
    }

    /**
     * Write given key/value to batch file
     *
     * @param guid guid of object
     * @param bytes object to be stored
     * @return start position of entry in file
     * @throws IOException
     */
    @Override
    public long write(String guid, byte[] bytes) throws IOException {
        RandomAccessFile accessFile = new RandomAccessFile(file, "rw");
        long pos = file.length();
        accessFile.seek(pos);
        write(accessFile, guid, bytes);
        accessFile.close();
        return pos;
    }

    /**
     * Write given key/value to given batch file
     *
     * @param guid guid of object
     * @param bytes object to be stored
     * @return start position of entry in file
     * @throws IOException
     */
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

    /**
     * Implementation of file defragmentation via copying of all active entries to new file.
     * Since iterator returns only 'active' entries, we iterate over old file and save
     * all given entries into new file. After that we rename new file to old name and
     * remove old file.
     *
     * @return positions of 'active' entries in new file
     * @throws IOException
     */
    @Override
    protected Map<String, Long> innerDefragment() throws IOException {
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

    /**
     * Implementation of iterator over binary batch file
     */
    class BinaryBatchIterator extends AbstractFileBatchIterator {

        /**
         * Returns start position of next entry. In binary case, start position of next entry
         * is just at the end of current entry. Since RandomAccessFile at start object position,
         * we just 'jump' to the end on value.
         *
         * @param randomAccessFile file with start position at previous value
         * @param seek size of previous value
         * @return
         * @throws IOException
         */
        @Override
        protected long nextPos(RandomAccessFile randomAccessFile, long seek) throws IOException {
            return randomAccessFile.getFilePointer() + seek;
        }

        /**
         * Read state, guid and value size. State is first byte, guid is described
         * by guid size next to state byte. Integer after guid identifies value size.
         *
         * @param randomAccessFile file with start position at current entry
         * @return is entry in 'active' state
         * @throws IOException
         */
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

        /**
         * Mark entry as 'deleted'. Since state is identified by first byte in entry,
         * we just rewrite it to '0'
         *
         * @param randomAccessFile
         * @throws IOException
         */
        @Override
        protected void markDeleted(RandomAccessFile randomAccessFile) throws IOException {
            randomAccessFile.write(0);
        }

        /**
         * Compute the current entry size
         */
        @Override
        protected long currentEntrySize(String guid, int seek) {
            return seek + String.valueOf(guid.length()).length() + guid.length() + 3 +
                    String.valueOf(seek).length(); //todo: check
        }

        /**
         * Read value from current entry.
         *
         * @param randomAccessFile file with position at start of the saved object
         * @param seek size of value
         * @return value
         * @throws IOException
         */
        @Override
        protected byte[] readValue(RandomAccessFile randomAccessFile, int seek) throws IOException {
            byte[] bytes = new byte[seek];
            randomAccessFile.read(bytes);
            return bytes;
        }

    }
}
