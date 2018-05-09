package ru.zudin.objectstore.impl;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import ru.zudin.objectstore.BatchIterator;

import java.io.*;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of file batch that stores objects as strings.
 *
 * Entry in this case contains of:
 * - state
 * - guid
 * - object size
 * - object itself as Base64 encoded string
 *
 * Entry takes 2 lines, first line contains of state, guid and object size,
 * divided by space. Second line contains encoded value.
 *
 * @author Sergey Zudin
 * @since 08.05.18.
 */
class Base64Batch extends AbstractFileBatch {

    private PrintWriter printWriter;

    public Base64Batch(String path, String name, double sizeLoadFactor, long fileSizeThreshold) {
        super(path, name, sizeLoadFactor, fileSizeThreshold);
        this.printWriter = null;
    }

    @Override
    public void close() throws IOException {
        if (printWriter != null) {
            printWriter.close();
        }
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
        PrintWriter writer = getPrintWriter();
        return write(writer, guid, bytes);
    }

    /**
     * Write given key/value to given batch file
     *
     * @param guid guid of object
     * @param bytes object to be stored
     * @return start position of entry in file
     * @throws IOException
     */
    private long write(PrintWriter writer, String guid, byte[] bytes) {
        String encoded = Base64.getEncoder().encodeToString(bytes);
        long pos = fileSize();
        writer.println("1 " + guid + Base64BatchIterator.DIVISOR + encoded.length());
        writer.println(encoded);
        writer.flush();
        return pos;
    }

    @Override
    protected BatchIterator innerCreateIterator() {
        return new Base64BatchIterator();
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
        PrintWriter clearWriter = new PrintWriter(newFile);
        BatchIterator oldIterator = createIterator();
        while (oldIterator.hasNext()) {
            long pos = newFile.length();
            String guid = oldIterator.next();
            byte[] bytes = oldIterator.value();
            write(clearWriter, guid, bytes);
            positions.put(guid, pos);
        }
        File tempOld = new File(file.getPath() + ".old");
        file.renameTo(tempOld);
        newFile.renameTo(file);
        tempOld.delete();
        if (printWriter != null) {
            printWriter.close();
            printWriter = null;
        }
        return positions;
    }

    private PrintWriter getPrintWriter() throws IOException {
        if (printWriter == null) {
            printWriter = createPrintWriter();
        }
        return printWriter;
    }

    private PrintWriter createPrintWriter() throws IOException {
        if (!file.exists()) {
            if (!file.createNewFile()) {
                throw new IOException("Cannot create file '" + getName() + "'");
            }
        }
        return new PrintWriter(new FileOutputStream(file, true));
    }

    /**
     * Implementation of iterator over Base64 batch file
     */
    class Base64BatchIterator extends AbstractFileBatchIterator {

        private static final String DIVISOR = " ";

        /**
         * Returns start position of next entry. In Base64 case, start position of next entry
         * is on next line ofter value line. Since RandomAccessFile at start object position,
         * we just 'jump' to the end on value plus 1 (new line character).
         *
         * @param randomAccessFile file with start position at previous value
         * @param seek size of previous value
         * @return
         * @throws IOException
         */
        @Override
        protected long nextPos(RandomAccessFile randomAccessFile, long seek) throws IOException {
            return seek == 0 ? randomAccessFile.getFilePointer() : randomAccessFile.getFilePointer() + seek + 1;
        }

        /**
         * Read state, guid and value size. First line of entry contains of state, guid and
         * value size, divided by space.
         *
         * @param randomAccessFile file with start position at current entry
         * @return is entry in 'active' state
         * @throws IOException
         */
        @Override
        protected boolean readKeyAndGetStatus(RandomAccessFile randomAccessFile) throws IOException {
            String line = randomAccessFile.readLine();
            if (StringUtils.isBlank(line)) {
                return false;
            }
            String[] split = line.split(DIVISOR);
            guid = split[1];
            seek = Integer.parseInt(split[2]);
            return BooleanUtils.toBoolean(Integer.parseInt(split[0]));
        }

        /**
         * Mark entry as 'deleted'. Since state is identified by first character in entry,
         * we just rewrite it to '0'
         *
         * @param randomAccessFile
         * @throws IOException
         */
        @Override
        protected void markDeleted(RandomAccessFile randomAccessFile) throws IOException {
            randomAccessFile.writeBytes("0");
        }

        /**
         * Compute the current entry size
         */
        @Override
        protected long currentEntrySize(String guid, int seek) {
            return seek + guid.length() + 3 + String.valueOf(seek).length();
        }

        /**
         * Read value from current entry. In Base64 case, value is placed on new line.
         *
         * @param randomAccessFile file with position at start of the saved object
         * @param seek size of value
         * @return
         * @throws IOException
         */
        @Override
        protected byte[] readValue(RandomAccessFile randomAccessFile, int seek) throws IOException {
            String str = randomAccessFile.readLine();
            return Base64.getDecoder().decode(str);
        }

    }
}
