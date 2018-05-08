package ru.zudin.objectstore.impl;

import ru.zudin.objectstore.Batch;
import ru.zudin.objectstore.BatchIterator;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * @author sergey
 * @since 09.05.18
 */
abstract class AbstractBatch implements Batch {

    private final String name;
    private final double sizeLoadFactor;
    private final long fileSizeThreshold;
    protected final File file;

    public AbstractBatch(String path, String name, double sizeLoadFactor, long fileSizeThreshold) {
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
    public Optional<Map<String, Long>> defragmentIfNeeded() throws IOException {
        if (isDefragmentationNeeded()) {
            return Optional.of(defragment());
        } else {
            return Optional.empty();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o.getClass().equals(getClass()))) return false;

        AbstractBatch batch = (AbstractBatch) o;

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

}
