package ru.zudin.objectstore.impl;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import ru.zudin.objectstore.Batch;
import ru.zudin.objectstore.BatchIterator;

import java.io.*;
import java.util.*;

/**
 * @author Sergey Zudin
 * @since 08.05.18.
 */
class Base64Batch implements Batch {

    private final String path;
    private final String name;
    private final File file;
    private final double sizeLoadFactor;
    private final long fileSizeThreshold;
    private PrintWriter printWriter;
    private int removedNumber;
    private int removedSize;

    public Base64Batch(String path, String name, double sizeLoadFactor, long fileSizeThreshold) {
        this.path = path;
        this.name = name;
        this.fileSizeThreshold = fileSizeThreshold;
        this.file = new File(path + name);
        this.printWriter = null;
        this.removedNumber = 0;
        this.removedSize = 0;
        this.sizeLoadFactor = sizeLoadFactor;
    }

    @Override
    public String getName() {
        return name;
    }

    private PrintWriter getPrintWriter() throws IOException {
        if (printWriter == null) {
            printWriter = createPrintWriter();
        }
        return printWriter;
    }

    @Override
    public long write(String guid, byte[] bytes) throws IOException {
        PrintWriter writer = getPrintWriter();
        return write(writer, guid, bytes);
    }

    @Override
    public void delete(long pos) throws IOException {
        BatchIterator iterator = createIterator();
        iterator.setStartPos(pos);
        iterator.next();
        iterator.remove();
        iterator.close();
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
    public void delete(Set<String> guids) {
        BatchIterator iterator = createIterator();
        while (iterator.hasNext()) {
            String next = iterator.next();
            if (guids.contains(next)) {
                iterator.remove();
            }
        }
    }

    @Override
    public Optional<byte[]> get(long pos) throws IOException {
        BatchIterator iterator = createIterator();
        iterator.setStartPos(pos);
        iterator.next();
        byte[] value = iterator.value();
        iterator.close();
        return Optional.of(value);
    }

    @Override
    public BatchIterator createIterator() {
        return new Base64BatchIterator();
    }

    @Override
    public long fileSize() {
        return file.length();
    }

    @Override
    public long validSize() {
        return file.length() - removedSize;
    }

    @Override
    public Optional<Map<String, Long>> defragmentIfNeeded() throws IOException {
        if (isDefragmentationNeeded()) {
            return Optional.of(defragment());
        } else {
            return Optional.empty();
        }
    }

    private boolean isDefragmentationNeeded() {
        double fileSize = (double) fileSize();
        double proportion = removedSize / fileSize;
        return proportion >= sizeLoadFactor || (fileSize > fileSizeThreshold && removedSize != 0);
//        if (proportion < sizeLoadFactor) {
//            double avgSize = removedSize / (double) removedNumber; //todo: correct?
//            double totalNumber = fileSize / avgSize; //todo: if total size is small - ignore?
//            return removedNumber / totalNumber >= 0.33;
//        }
//        return true;
    }

    @Override
    public Map<String, Long> defragment() throws IOException {
        System.out.println("Defragmentation start for " + name);
        long start = System.currentTimeMillis();
        Map<String, Long> positions = new HashMap<>();
        File newFile = new File(path + name + ".new");
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
        removedNumber = 0;
        removedSize = 0;
        long elapsed = System.currentTimeMillis() - start;
        System.out.println(String.format("Defragmentation finish for %s, took %d millis", name, elapsed));
        return positions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Base64Batch)) return false;

        Base64Batch batch = (Base64Batch) o;

        return name.equals(batch.name);

    }
    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public void close() throws IOException {
        if (printWriter != null) {
            printWriter.close();
        }
    }

    private PrintWriter createPrintWriter() throws IOException {
        if (!file.exists()) {
            if (!file.createNewFile()) {
                throw new IOException("Cannot create file '" + name + "'");
            }
        }
        return new PrintWriter(new FileOutputStream(file, true));
    }

    public class Base64BatchIterator implements BatchIterator {
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
                Base64Batch.this.removedNumber++;
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
