package ru.zudin.objectstore.impl;

import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang.SerializationUtils;
import ru.zudin.objectstore.AppendOnlyObjectStore;
import ru.zudin.objectstore.Batch;
import ru.zudin.objectstore.BatchIterator;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author sergey
 * @since 07.05.18
 */
public class FileSystemObjectStore implements AppendOnlyObjectStore, Closeable {

    private final String folder;
    private final int initBatchSize;
    private final double sizeLoadFactor;
    private final long fileSizeThreshold;
    private final BatchType batchType;

    private Map<String, Position> index;
    private List<Batch> batches;

    public FileSystemObjectStore(String folder) {
        this(folder, BatchType.BASE_64);
    }

    public FileSystemObjectStore(String folder, BatchType batchType) {
        this(folder, batchType, 16);
    }

    public FileSystemObjectStore(String folder, BatchType batchType, int initBatchSize) {
        this(folder, batchType, initBatchSize, 0.33, 1024 * 1024 * 200);
    }

    public FileSystemObjectStore(String folder, BatchType batchType, int initBatchSize, double sizeLoadFactor, long fileSizeThreshold) {
        //todo: check batch size
        if (!folder.endsWith("/")) {
            folder += "/";
        }
        this.folder = folder;
        this.batchType = batchType;
        this.index = new HashMap<>();
        this.batches = new ArrayList<>();
        this.initBatchSize = initBatchSize;
        this.sizeLoadFactor = sizeLoadFactor;
        this.fileSizeThreshold = fileSizeThreshold;
    }

    private void lazyInit() throws IOException {
        if (batches.isEmpty()) {
            scan();
            createBatches(initBatchSize);
        }
    }

    @Override
    public String put(Serializable object) throws IOException {
        lazyInit();
        String guid = generateGuid();
        byte[] bytes = SerializationUtils.serialize(object);
        Batch batch = selectBatch(guid);
        long pos = batch.write(guid, bytes);
        index.put(guid, new Position(batch, pos));
        rebalanceIfNeeded(batch);
        return guid;
    }

    private void rebalanceIfNeeded(Batch batch) throws IOException {
        if (batch.fileSize() > fileSizeThreshold) {
            System.out.println(String.format("Start re-balance, init size=%d, make=%d", batches.size(), batches.size() * 2));
            long start = System.currentTimeMillis();
            List<Batch> created = createBatches(this.batches.size() * 2);
            List<Batch> oldies = ListUtils.subtract(batches, created);
            double averageSize = batches.stream()
                    .mapToLong(Batch::validSize)
                    .average()
                    .getAsDouble();
            int i = 0;
            int j = 0;
            Batch from = oldies.get(i);
            Batch to = created.get(j);
            BatchIterator fromIterator = from.createIterator();
            while (true) {
                while (fromIterator.hasNext() && from.validSize() > averageSize && to.validSize() <= averageSize) {
                    String guid = fromIterator.next();
                    byte[] bytes = fromIterator.value();
                    long newPos = to.write(guid, bytes);
                    index.put(guid, new Position(to, newPos));
                    fromIterator.remove();
                }
                if (from.validSize() <= averageSize) {
                    i++;
                    if (i < oldies.size()) {
                        fromIterator.close();
                        from = oldies.get(i);
                        fromIterator = from.createIterator();
                    } else {
                        break;
                    }
                } else {
                    j++;
                    if (j < created.size()) {
                        to = created.get(j);
                    } else {
                        break;
                    }
                }
            }
            fromIterator.close();
            long elapsed = System.currentTimeMillis() - start;
            System.out.println(String.format("Finish re-balance, took %d", elapsed));
            for (Batch old : oldies) {
                defragmentIfNeeded(old);
            }
        }
    }

    @Override
    public Optional<Object> get(String guid) throws IOException {
        lazyInit();
        Position position = index.get(guid);
        if (position == null) {
            return Optional.empty();
        }
        Batch batch = position.getBatch();
        Optional<byte[]> optional = batch.get(position.getPos());
        if (!optional.isPresent()) {
            return Optional.empty();
        }
        return Optional.of(SerializationUtils.deserialize(optional.get()));
    }

    @Override
    public void delete(String guid) throws IOException {
        lazyInit();
        Position position = index.get(guid);
        if (position != null) {
            Batch batch = position.getBatch();
            batch.delete(position.getPos());
            index.remove(guid);
            defragmentIfNeeded(batch);
        }
    }

    private boolean defragmentIfNeeded(Batch batch) throws IOException {
        Optional<Map<String, Long>> optMap = batch.defragmentIfNeeded();
        if (optMap.isPresent()) {
            Map<String, Long> map = optMap.get();
            for (String guid : map.keySet()) {
                Position position = index.get(guid);
                Long newPos = map.get(guid);
                if (position == null) {
                    System.out.println(String.format("Wrong behaviour: guid #%s in file  (%s), but not in index",
                            guid, batch.getName()));
                    index.put(guid, new Position(batch, newPos));
                } else {
                    position.setPos(newPos);
                }
            }
        }
        return optMap.isPresent();
    }

    @Override
    public void delete(Collection<String> guids) throws IOException {
        lazyInit();
        Map<Batch, Set<String>> grouped = guids.stream()
                .distinct()
                .filter(index::containsKey)
                .collect(Collectors.groupingBy(g -> index.get(g).getBatch(), Collectors.toSet()));
        for (Batch batch : grouped.keySet()) {
            Set<String> toDelete = grouped.get(batch);
            batch.delete(toDelete);
            toDelete.forEach(index::remove);
            defragmentIfNeeded(batch);
        }
    }

    @Override
    public void close() throws IOException {
        for (Batch batch : batches) {
            batch.close();
        }
    }

    /* START TESTING */
    protected void clear() {
        File[] files = getFiles();
        for (File file : files) {
            file.delete();
        }
        createBatches(initBatchSize);
    }

    protected List<Batch> getBatches() {
        return batches;
    }
    /* END TESTING */

    private String encodeValue(Serializable object) {
        return Base64.getEncoder().encodeToString(SerializationUtils.serialize(object));
    }

    private Object decodeValue(String value) {
        return SerializationUtils.deserialize(Base64.getDecoder().decode(value));
    }

    private String generateGuid() {
        while (true) {
            String hex = UUID.randomUUID().toString();
            if (!index.containsKey(hex)) {
                return hex;
            }
        }
    }

    private Batch selectBatch(String guid) {
        return batches.get(Math.abs(guid.hashCode()) % batches.size());
    }

    private boolean scan() throws IOException {
        File[] files = getFiles();
        if (files.length == 0) {
            return false;
        } else {
            for (File file : files) {
                String fileName = file.getName();
                String extention = fileName.split("\\.")[1];
                BatchType batchType = null;
                for (BatchType type : BatchType.values()) {
                    if (type.getExtention().equals(extention)) {
                        batchType = type;
                        break;
                    }
                }
                if (batchType == null) {
                    continue;
                }
                Batch batch = getBatch(fileName);
                Map<String, Long> defragment = batch.defragment();
                for (String guid : defragment.keySet()) {
                    index.put(guid, new Position(batch, defragment.get(guid)));
                }
                batches.add(batch);
            }
            return true;
        }
    }

    private File[] getFiles() {
        File path = new File(folder);
        if (!path.isDirectory()) {
            throw new IllegalArgumentException("Folder is invalid");
        }
        Pattern pattern = Pattern.compile("batch-\\d+\\..+"); //todo: support types
        return path.listFiles(pathname -> pattern.matcher(pathname.getName()).matches());
    }

    private List<Batch> createBatches(int batchSize) {
        int i = 0;
        List<Batch> created = new ArrayList<>();
        while (batches.size() < batchSize) {
            Batch batch;
            while (true) {
                String name = "batch-" + i++ + "." + batchType.getExtention();
                batch = getBatch(name);
                if (!batches.contains(batch)) {
                    break;
                }
            }
            batches.add(batch);
            created.add(batch);
        }
        return created;
    }

    private Batch getBatch(String name) {
        Batch batch;
        if (batchType == BatchType.BASE_64) {
            batch = new Base64Batch(folder, name, sizeLoadFactor, fileSizeThreshold);
        } else if (batchType == BatchType.BINARY) {
            batch = new BinaryBatch(folder, name, sizeLoadFactor, fileSizeThreshold);
        } else {
            throw new IllegalStateException("Unsupported type " + batchType);
        }
        return batch;
    }

    private class Position {
        private Batch batch;
        private long pos;

        public Position(Batch batch, long pos) {
            this.batch = batch;
            this.pos = pos;
        }

        public Batch getBatch() {
            return batch;
        }

        public long getPos() {
            return pos;
        }

        public void setPos(long pos) {
            this.pos = pos;
        }
    }

    public enum BatchType {
        BINARY("bnos"),
        BASE_64("bsos");

        private String extention;

        BatchType(String extention) {
            this.extention = extention;
        }

        public String getExtention() {
            return extention;
        }
    }
}
