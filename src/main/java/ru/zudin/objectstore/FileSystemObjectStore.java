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
    private Map<String, Batch> index;
    private List<Batch> batches;

    public FileSystemObjectStore(String folder) {
        this(folder, 4);
    }

    public FileSystemObjectStore(String folder, int initBatchSize) {
        //todo: check batch size
        if (!folder.endsWith("/")) {
            folder += "/";
        }
        this.folder = folder;
        this.index = new HashMap<>();
        this.batches = new ArrayList<>();
        this.initBatchSize = initBatchSize;
    }

    private void lazyInit() {
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
        batch.write(guid, serializedValue);
//        times[i++] = System.currentTimeMillis();
        index.put(guid, batch);
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
        Batch batch = index.get(guid);
        if (batch == null) {
            return Optional.empty();
        }
        try {
            Batch.BatchIterator iterator = batch.createIterator();
            while (iterator.hasNext()) {
                String savedGuid = iterator.next();
                if (guid.equals(savedGuid)) {
                    String value = iterator.currentValue();
                    return Optional.of(decodeValue(value));
                }
            }
        } catch (IllegalStateException e) {
            throw new IOException(e);
        }
        //wrong behaviour: guid in index, but not in file
        index.remove(guid);
        return Optional.empty();
    }

    @Override
    public void delete(String guid) {
        lazyInit();
        Batch batch = index.get(guid);
        if (batch != null) {
            Batch.BatchIterator iterator = batch.createIterator();
            while (iterator.hasNext()) {
                String savedGuid = iterator.next();
                if (guid.equals(savedGuid)) {
                    iterator.remove();
                    break;
                }
            }
        }
    }

    @Override
    public void delete(Collection<String> guids) {
        lazyInit();
        Map<Batch, Set<String>> grouped = guids.stream()
                .distinct()
                .filter(index::containsKey)
                .collect(Collectors.groupingBy(index::get, Collectors.toSet()));
        for (Batch batch : grouped.keySet()) {
            batch.delete(grouped.get(batch));
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

    private boolean scan() {
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

    private void fillIndex(Batch batch) {
        Batch.BatchIterator iterator = batch.createIterator();
        while (iterator.hasNext()) {
            String guid = iterator.next();
            index.put(guid, batch);
        }
    }
}
