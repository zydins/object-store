package ru.zudin.objectstore.impl;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import ru.zudin.objectstore.BatchIterator;

import java.io.*;
import java.util.*;

/**
 * @author Sergey Zudin
 * @since 08.05.18.
 */
class Base64Batch extends AbstractBatch {

    private PrintWriter printWriter;
    protected int removedSize;

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
    public long validSize() {
        return file.length() - removedSize;
    }

    @Override
    public Map<String, Long> defragment() throws IOException {
        System.out.println("Defragmentation start for " + getName());
        long start = System.currentTimeMillis();
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
        long elapsed = System.currentTimeMillis() - start;
        System.out.println(String.format("Defragmentation finish for %s, took %d millis", getName(), elapsed));
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

    class Base64BatchIterator implements BatchIterator {
        static final String DIVISOR = " ";

        private RandomAccessFile randomAccessFile;
        private String guid;
        private int seek;
        private long pos;
        private byte[] value;
        private Optional<Boolean> hasNext;

        public Base64BatchIterator() {
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

        private boolean iterate() throws IOException {
            while (true) {
                pos = randomAccessFile.getFilePointer() + seek + 1;
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

        @Override
        public void remove() {
            try {
                long prev = randomAccessFile.getFilePointer();
                randomAccessFile.seek(pos);
                randomAccessFile.writeBytes("0"); //mark removed
                randomAccessFile.seek(prev);
                Base64Batch.this.removedSize += seek + guid.length() + 3 + String.valueOf(seek).length();
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
                String str = randomAccessFile.readLine();
                value = Base64.getDecoder().decode(str);
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

        private void init() {
            if (randomAccessFile == null) {
                try {
                    randomAccessFile = new RandomAccessFile(file, "rw");
                } catch (FileNotFoundException e) {
                    throw new IllegalStateException("File is not found: '" + file.getPath() + "");
                }
            }
        }

        @Override
        public void close() throws IOException {
            randomAccessFile.close();
        }
    }
}
