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
 * Common file batch implementation
 *
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

    /**
     * Get object via setting the start position in iterator
     */
    @Override
    public Optional<byte[]> get(long pos) throws IOException {
        //todo: check that position is valid
        BatchIterator iterator = createIterator();
        iterator.setStartPos(pos);
        byte[] value = null;
        if (iterator.hasNext()) {
            iterator.next();
            value = iterator.value();
        }
        iterator.close();
        return Optional.ofNullable(value);
    }

    /**
     * Mark object deleted via setting the start position in iterator
     */
    @Override
    public void delete(long pos) throws IOException {
        BatchIterator iterator = createIterator();
        iterator.setStartPos(pos);
        if (iterator.hasNext()) {
            iterator.next();
            iterator.remove();
        }
        iterator.close();
    }

    /**
     * Mark objects deleted via iteration over file
     */
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

    /**
     * Call defragmentation if precondition is met
     * @return new positions of objects
     */
    @Override
    public Optional<Map<String, Long>> defragmentIfNeeded() throws IOException {
        if (isDefragmentationNeeded()) {
            return Optional.of(defragment());
        } else {
            return Optional.empty();
        }
    }

    /**
     * Defragment file if proportion of deleted size to total size is greater that fixed factor
     */
    private boolean isDefragmentationNeeded() {
        double fileSize = (double) fileSize();
        double proportion = validSize() / fileSize;
        return 1 - proportion >= sizeLoadFactor || (fileSize > fileSizeThreshold && 1 - validSize() != 0);
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

    /**
     * Real implementation of defragmentation
     */
    protected abstract Map<String,Long> innerDefragment() throws FileNotFoundException, IOException;

    @Override
    public BatchIterator createIterator() throws IOException {
        if (!file.exists()) {
            throw new IOException(String.format("File '%s' is not exists", file.getName()));
        }
        return innerCreateIterator();
    }

    /**
     * Real creation of iterator
     */
    protected abstract BatchIterator innerCreateIterator();

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

    /**
     * Common realisation of batch iterator. It is used a RandomAccessFile object
     * to iterate over file entries. Entry contains key (guid), value (object),
     * status (active/deleted), size of value in bytes and some meta-information which depends on realisation.
     * Use of RandomAccessFile allows to 'jump' from key position to value position and back.
     * That, in other hand, allows to read objects only when it is required and do not use
     * unnecessary space when it is possible.
     */
    protected abstract class AbstractFileBatchIterator implements BatchIterator {
        private RandomAccessFile randomAccessFile;
        protected String guid;
        protected int seek;
        private long pos;
        private byte[] value;
        private Optional<Boolean> hasNext;
        private boolean wasNext;
        private boolean wasRemove;

        public AbstractFileBatchIterator() {
            this.randomAccessFile = null;
            this.seek = 0;
            this.pos = 0;
            this.guid = null;
            this.hasNext = Optional.empty();
            wasNext = false;
            wasRemove = false;
        }

        /**
         * Lazy initialization of RandomAccessFile
         */
        private void init() {
            if (randomAccessFile == null) {
                try {
                    randomAccessFile = new RandomAccessFile(file, "rw");
                } catch (FileNotFoundException e) {
                    throw new IllegalStateException("File is not found: '" + file.getPath() + "");
                }
            }
        }

        /**
         * Jump to the next entry in file
         *
         * @return is there next entry
         * @throws IOException
         */
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

        /**
         * Get start position of next entry
         *
         * @param randomAccessFile file with start position at previous value
         * @param seek size of previous value
         * @return start position of next entry
         * @throws IOException
         */
        protected abstract long nextPos(RandomAccessFile randomAccessFile, long seek) throws IOException;

        /**
         * Read current status, key (guid), size of value and meta-information.
         * This method MUST rewrite 'guid' and 'seek' fields.
         *
         * @param randomAccessFile file with start position at current entry
         * @return status of entry
         * @throws IOException
         */
        protected abstract boolean readKeyAndGetStatus(RandomAccessFile randomAccessFile) throws IOException;

        /**
         * Checks if there is a next active element. It have to read next entry to
         * jump over entries with 'deleted' state.
         */
        @Override
        public boolean hasNext() {
            init();
            try {
                if (hasNext.isPresent()) {
                    return hasNext.get();
                }
                boolean iterate = iterate();
                this.hasNext = Optional.of(iterate);
                wasNext = false;
                return iterate;
            } catch (IOException e) {
                throw new IllegalStateException("Cannot read file '" + file.getPath() + "");
            }
        }

        /**
         * Returns key (guid) of entry. If there was call of 'hasNext' method, than return extracted value.
         * Otherwise, read next entry.
         *
         * @return guid of the entry
         */
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
                wasNext = true;
                wasRemove = false;
                return guid;
            } catch (IOException e) {
                throw new IllegalStateException("Cannot read file: '" + file.getPath() + "");
            }
        }

        /**
         * Mark current entry as 'deleted' and updates size of removed entities.
         * Do not change current position. Might be called ONLY after 'next' method call.
         */
        @Override
        public void remove() {
            if (!wasNext || wasRemove) {
                throw new IllegalStateException("Next() method has not yet been called");
            }
            try {
                long prev = randomAccessFile.getFilePointer();
                randomAccessFile.seek(pos);
                markDeleted(randomAccessFile);
                removedSize += updateRemovedSize(guid, seek);
                randomAccessFile.seek(prev);
                wasRemove = true;
            } catch (IOException e) {
                throw new IllegalStateException("Cannot read file: '" + file.getPath() + "");
            }
        }

        /**
         * Marks current entry as 'deleted'
         */
        protected abstract void markDeleted(RandomAccessFile randomAccessFile) throws IOException;

        /**
         * Updates a size of 'deleted' entries
         */
        protected abstract long updateRemovedSize(String guid, int seek);

        /**
         * Returns value (saved object) of current entry. Might be called ONLY after 'next' method call.
         */
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

        /**
         * Reads value with size of 'seek' from file with position at start of the saved object.
         * @param randomAccessFile file with position at start of the saved object
         * @param seek size of value
         * @throws IOException
         */
        protected abstract byte[] readValue(RandomAccessFile randomAccessFile, int seek) throws IOException;

        /**
         * Returns position at start of current entry
         */
        @Override
        public long pos() {
            return pos;
        }

        /**
         * Sets start position for RandomAccessFile
         * @throws IOException
         */
        @Override
        public void setStartPos(long pos) throws IOException {
            init();
            randomAccessFile.seek(pos);
        }

        @Override
        public void close() throws IOException {
            if (randomAccessFile != null) {
                randomAccessFile.close();
            }
        }

    }

}
