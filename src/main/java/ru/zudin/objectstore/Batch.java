package ru.zudin.objectstore;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;

import java.io.*;
import java.util.*;

/**
 * @author Sergey Zudin
 * @since 08.05.18.
 */
class Batch implements Closeable {

    private final String path;
    private final String name;
    private PrintWriter printWriter;
    private int removedNumber;
    private int removedSize;

    public Batch(String path, String name) {
        this.path = path;
        this.name = name;
        this.printWriter = null;
        this.removedNumber = 0;
        this.removedSize = 0;
    }

    public String getName() {
        return name;
    }

    private PrintWriter getPrintWriter() throws IOException {
        if (printWriter == null) {
            printWriter = createPrintWriter();
        }
        return printWriter;
    }

    public long write(String guid, String encoded) throws IOException {
        PrintWriter writer = getPrintWriter();
        return write(writer, guid, encoded);
    }

    private long write(PrintWriter writer, String guid, String encoded) {
        long pos = fileSize();
        writer.println("1 " + guid + BatchIterator.DIVISOR + encoded.length());
        writer.println(encoded);
        writer.flush();
        return pos;
    }

    public void delete(Set<String> guids) {
        BatchIterator iterator = createIterator();
        while (iterator.hasNext()) {
            String next = iterator.next();
            if (guids.contains(next)) {
                iterator.remove();
            }
        }
    }

    public BatchIterator createIterator() {
        return new BatchIterator(getFile());
    }

    protected long fileSize() {
        File file = getFile();
        return file.length();
    }

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
        if (proportion < 0.33) {
            double avgSize = removedSize / (double) removedNumber; //todo: not correct
            double totalNumber = fileSize / avgSize; //todo: if total size is small - ignore?
            return removedNumber / totalNumber >= 0.33;
        }
        return true;
    }

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
            String value = oldIterator.currentValue();
            write(clearWriter, guid, value);
            positions.put(guid, pos);
        }
        File oldFile = getFile();
        File tempOld = new File(oldFile.getPath() + ".old");
        oldFile.renameTo(tempOld);
        newFile.renameTo(oldFile);
        tempOld.delete();
        printWriter.close();
        printWriter = null;
        removedNumber = 0;
        removedSize = 0;
        long elapsed = System.currentTimeMillis() - start;
        System.out.println(String.format("Defragmentation finish for %s, took %d millis", name, elapsed));
        return positions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Batch)) return false;

        Batch batch = (Batch) o;

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
        File file = getFile();
        if (!file.exists()) {
            if (!file.createNewFile()) {
                throw new IOException("Cannot create file '" + name + "'");
            }
        }
        return new PrintWriter(new FileOutputStream(file, true));
    }

    private File getFile() {
        return new File(path + name);
    }

    public class BatchIterator implements Iterator, Closeable {
        private static final String DIVISOR = " ";

        private final File store;
        private RandomAccessFile randomAccessFile;
        private String guid;
        private int seek;
        private long pos;
        private String value;
        private Optional<Boolean> hasNext;

        public BatchIterator(File store) {
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
                Batch.this.removedNumber++;
                Batch.this.removedSize += seek + guid.length() + 3 + String.valueOf(seek).length();
            } catch (IOException e) {
                throw new IllegalStateException("Cannot read file: '" + store.getPath() + "");
            }
        }

        public String currentValue() {
            if (value != null) {
                return value;
            }
            try {
                long prev = randomAccessFile.getFilePointer();
                value = randomAccessFile.readLine();
                randomAccessFile.seek(prev);
                return value;
            } catch (IOException e) {
                throw new IllegalStateException("Cannot read file: '" + store.getPath() + "");
            }
        }

        public long getPos() {
            return pos;
        }

        public void setStartPos(long pos) throws IOException {
            init();
            randomAccessFile.seek(pos);
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

        @Override
        public void close() throws IOException {
            randomAccessFile.close();
        }
    }
}
