package ru.zudin.objectstore.impl;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import ru.zudin.objectstore.BatchIterator;

import java.io.*;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
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

    @Override
    public long write(String guid, byte[] bytes) throws IOException {
        PrintWriter writer = getPrintWriter();
        return write(writer, guid, bytes);
    }

    private long write(PrintWriter writer, String guid, byte[] bytes) {
        String encoded = Base64.getEncoder().encodeToString(bytes);
        long pos = fileSize();
        writer.println("1 " + guid + Base64BatchIterator.DIVISOR + encoded.length());
        writer.println(encoded);
        writer.flush();
        return pos;
    }

    @Override
    public BatchIterator createIterator() {
        return new Base64BatchIterator();
    }

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

    class Base64BatchIterator extends AbstractFileBatchIterator {
        static final String DIVISOR = " ";

        @Override
        protected long nextPos(RandomAccessFile randomAccessFile, long seek) throws IOException {
            return seek == 0 ? randomAccessFile.getFilePointer() : randomAccessFile.getFilePointer() + seek + 1;
        }

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

        @Override
        protected void markDeleted(RandomAccessFile randomAccessFile) throws IOException {
            randomAccessFile.writeBytes("0");
        }

        @Override
        protected long updateRemovedSize(String guid, int seek) {
            return seek + guid.length() + 3 + String.valueOf(seek).length();
        }

        @Override
        protected byte[] readValue(RandomAccessFile randomAccessFile, int seek) throws IOException {
            String str = randomAccessFile.readLine();
            return Base64.getDecoder().decode(str);
        }

    }
}
