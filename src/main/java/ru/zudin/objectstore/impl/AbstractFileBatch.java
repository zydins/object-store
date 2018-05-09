package ru.zudin.objectstore.impl;

import ru.zudin.objectstore.Batch;
import ru.zudin.objectstore.BatchIterator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;

/**
 * @author sergey
 * @since 09.05.18
 */
abstract class AbstractFileBatch implements Batch {

    private final String name;
    private final double sizeLoadFactor;
    private final long fileSizeThreshold;
    protected final File file;
    private long removedSize;

    public AbstractFileBatch(String path, String name, double sizeLoadFactor, long fileSizeThreshold) {
        this.name = name;
        this.fileSizeThreshold = fileSizeThreshold;
        this.file = new File(path + name);
        this.sizeLoadFactor = sizeLoadFactor;
    }

    @Override
    public String getName() {
        return name;
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
    public void delete(long pos) throws IOException {
        BatchIterator iterator = createIterator();
        iterator.setStartPos(pos);
        iterator.next();
        iterator.remove();
        iterator.close();
    }

    @Override
    public void delete(Set<String> guids) throws IOException {
        BatchIterator iterator = createIterator();
        while (iterator.hasNext()) {
            String next = iterator.next();
            if (guids.contains(next)) {
                iterator.remove();
            }
        }
        iterator.close();
    }

    @Override
    public long fileSize() {
        return file.length();
    }

    @Override
    public long validSize() {
        return fileSize() - removedSize;
    }

    @Override
    public Optional<Map<String, Long>> defragmentIfNeeded() throws IOException {
        if (isDefragmentationNeeded()) {
            return Optional.of(defragment());
        } else {
            return Optional.empty();
        }
    }

    @Override
    public Map<String, Long> defragment() throws IOException {
        System.out.println("Defragmentation start for " + getName());
        long start = System.currentTimeMillis();
        Map<String, Long> positions = innerDefragment();
        long elapsed = System.currentTimeMillis() - start;
        System.out.println(String.format("Defragmentation finish for %s, took %d millis", getName(), elapsed));
        removedSize = 0;
        return positions;
    }

    protected abstract Map<String,Long> innerDefragment() throws FileNotFoundException, IOException;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o.getClass().equals(getClass()))) return false;

        AbstractFileBatch batch = (AbstractFileBatch) o;

        return getName().equals(batch.getName());

    }
    @Override
    public int hashCode() {
        return getName().hashCode();
    }

    private boolean isDefragmentationNeeded() {
        double fileSize = (double) fileSize();
        double proportion = validSize() / fileSize;
        return 1 - proportion >= sizeLoadFactor || (fileSize > fileSizeThreshold && 1 - validSize() != 0);
//        if (proportion < sizeLoadFactor) {
//            double avgSize = removedSize / (double) removedNumber; //todo: correct?
//            double totalNumber = fileSize / avgSize; //todo: if total size is small - ignore?
//            return removedNumber / totalNumber >= 0.33;
//        }
//        return true;
    }

    protected abstract class AbstractFileBatchIterator implements BatchIterator {
        private RandomAccessFile randomAccessFile;
        protected String guid;
        protected int seek;
        private long pos;
        private byte[] value;
        private Optional<Boolean> hasNext;

        public AbstractFileBatchIterator() {
            this.randomAccessFile = null;
            this.seek = 0;
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
                pos = nextPos(randomAccessFile, seek);
                if (randomAccessFile.length() <= pos) {
                    return false;
                }
                randomAccessFile.seek(pos);
                boolean isActive = readKeyAndGetStatus(randomAccessFile);
                if (this.guid == null || this.seek < 0) {
                    throw new IllegalStateException("Guid or seek is not initiated");
                }
                if (!isActive) {
                    continue;
                }
                return true;
            }
        }


        protected abstract long nextPos(RandomAccessFile randomAccessFile, long seek) throws IOException;

        protected abstract boolean readKeyAndGetStatus(RandomAccessFile randomAccessFile) throws IOException;

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
                markDeleted(randomAccessFile);
                removedSize += updateRemovedSize(guid, seek);
                randomAccessFile.seek(prev);
            } catch (IOException e) {
                throw new IllegalStateException("Cannot read file: '" + file.getPath() + "");
            }
        }

        protected abstract void markDeleted(RandomAccessFile randomAccessFile) throws IOException;

        protected abstract long updateRemovedSize(String guid, int seek);

        @Override
        public byte[] value() {
            if (value != null) {
                return value;
            }
            try {
                long prev = randomAccessFile.getFilePointer();
                value = readValue(randomAccessFile, seek);
                randomAccessFile.seek(prev);
                return value;
            } catch (IOException e) {
                throw new IllegalStateException("Cannot read file: '" + file.getPath() + "");
            }
        }

        protected abstract byte[] readValue(RandomAccessFile randomAccessFile, int seek) throws IOException;

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
