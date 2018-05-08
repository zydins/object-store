package ru.zudin.objectstore;

import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;

import java.io.*;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;

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

    public void write(String guid, String encoded) throws IOException {
        PrintWriter writer = getPrintWriter();
        write(writer, guid, encoded);
    }

    private void write(PrintWriter writer, String guid, String encoded) {
        writer.println("1 " + guid + BatchIterator.DIVISOR + encoded.length());
        writer.println(encoded);
        writer.flush();
    }

    public void delete(Set<String> guids) {
        BatchIterator iterator = createIterator();
        while (iterator.hasNext()) {
            String next = iterator.next();
            if (guids.contains(next)) {
                iterator.remove();
                guids.remove(next);//
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

    public void defragment() throws IOException {
        File newFile = new File(path + name + ".new");
        PrintWriter clearWriter = new PrintWriter(newFile);
        BatchIterator oldIterator = createIterator();
        while (oldIterator.hasNext()) {
            String guid = oldIterator.next();
            String value = oldIterator.currentValue();
            write(clearWriter, guid, value);
        }
        File oldFile = getFile();
        File tempOld = new File(oldFile.getPath() + ".old");
        oldFile.renameTo(tempOld);
        newFile.renameTo(oldFile);
        tempOld.delete();
        printWriter.close();
        printWriter = null;
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

    public class BatchIterator implements Iterator {
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
                //todo: close file?
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
                Batch.this.removedSize += seek;
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
}
