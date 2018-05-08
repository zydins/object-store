package ru.zudin.objectstore;

import org.apache.commons.lang.SerializationUtils;

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
    private Map<String, Position> index;
    private List<Batch> batches;
    private boolean autoDefragmentation;

    public FileSystemObjectStore(String folder) {
        this(folder, 4);
    }

    public FileSystemObjectStore(String folder, int initBatchSize) {
        this(folder, initBatchSize, true);
    }

    public FileSystemObjectStore(String folder, int initBatchSize, boolean autoDefragmentation) {
        //todo: check batch size
        if (!folder.endsWith("/")) {
            folder += "/";
        }
        this.folder = folder;
        this.index = new HashMap<>();
        this.batches = new ArrayList<>();
        this.initBatchSize = initBatchSize;
        this.autoDefragmentation = autoDefragmentation;
    }

    private void lazyInit() throws IOException {
        if (batches.isEmpty()) {
            scan();
            createBatches();
        }
    }

    @Override
    public String put(Serializable object) throws IOException {
        lazyInit();
//        long[] times = new long[6];
//        int i = 0;
//        times[i++] = System.currentTimeMillis();
        String guid = generateGuid();
//        times[i++] = System.currentTimeMillis();
        //todo: big files?
        String serializedValue = encodeValue(object);
//        times[i++] = System.currentTimeMillis();
        Batch batch = getBatch(guid);
//        times[i++] = System.currentTimeMillis();
        long pos = batch.write(guid, serializedValue);
//        times[i++] = System.currentTimeMillis();
        index.put(guid, new Position(batch, pos));
//        times[i++] = System.currentTimeMillis();
//        StringBuilder builder = new StringBuilder();
//        for (int j = 1; j < times.length; j++) {
//            builder.append(times[j] - times[j - 1] + " / ");
//        }
//        System.out.println(builder.toString());
        return guid;
    }

    @Override
    public Optional<Object> get(String guid) throws IOException {
        lazyInit();
        Position position = index.get(guid);
        if (position == null) {
            return Optional.empty();
        }
        Batch batch = position.getBatch();
        Batch.BatchIterator iterator = null;
        try {
            iterator = batch.createIterator();
            iterator.setStartPos(position.getPos());
            while (iterator.hasNext()) {
                String savedGuid = iterator.next();
                if (guid.equals(savedGuid)) {
                    String value = iterator.currentValue();
                    iterator.close();
                    return Optional.of(decodeValue(value));
                }
            }
            //wrong behaviour: guid in index, but not in file
            index.remove(guid);
            return Optional.empty();
        } catch (IllegalStateException e) {
            throw new IOException(e);
        } finally {
            if (iterator != null) {
                iterator.close();
            }
        }
    }

    @Override
    public void delete(String guid) throws IOException {
        lazyInit();
        Position position = index.get(guid);
        if (position != null) {
            Batch batch = position.getBatch();
            Batch.BatchIterator iterator = batch.createIterator();
            iterator.setStartPos(position.getPos());
            while (iterator.hasNext()) {
                String savedGuid = iterator.next();
                if (guid.equals(savedGuid)) {
                    iterator.remove();
                    index.remove(guid);
                    break;
                }
            }
            iterator.close();
            defragmentIfNeeded(batch);
        }
    }

    private void defragmentIfNeeded(Batch batch) throws IOException {
        if (autoDefragmentation) {
            Optional<Map<String, Long>> optMap = batch.defragmentIfNeeded();
            if (optMap.isPresent()) {
                Map<String, Long> map = optMap.get();
                for (String guid : map.keySet()) {
                    Position position = index.get(guid);
                    Long newPos = map.get(guid);
                    if (position == null) {
                        //wrong behaviour: object in file but not in index
                        index.put(guid, new Position(batch, newPos));
                    } else {
                        position.setPos(newPos);
                    }
                }
            }
        }
    }

    public boolean isAutoDefragmentation() {
        return autoDefragmentation;
    }

    public void setAutoDefragmentation(boolean autoDefragmentation) {
        this.autoDefragmentation = autoDefragmentation;
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
        createBatches();
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

    private Batch getBatch(String guid) {
        return batches.get(Math.abs(guid.hashCode()) % batches.size());
    }

    private boolean scan() throws IOException {
        File[] files = getFiles();
        if (files.length == 0) {
            return false;
        } else {
            for (File file : files) {
                Batch batch = new Batch(folder, file.getName());
                fillIndex(batch);
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
        Pattern pattern = Pattern.compile("batch-\\d+\\.fsos");
        return path.listFiles(pathname -> pattern.matcher(pathname.getName()).matches());
    }

    private void createBatches() {
        int i = 0;
        while (batches.size() < initBatchSize) {
            Batch batch;
            while (true) {
                batch = new Batch(folder, "batch-" + i++ + ".fsos");
                if (!batches.contains(batch)) {
                    break;
                }
            }
            batches.add(batch);
        }
    }

    private void fillIndex(Batch batch) throws IOException {
        Batch.BatchIterator iterator = batch.createIterator();
        while (iterator.hasNext()) {
            String guid = iterator.next();
            long pos = iterator.getPos();
            index.put(guid, new Position(batch, pos));
        }
        iterator.close();
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
}
