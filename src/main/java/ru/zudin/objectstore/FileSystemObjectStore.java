package ru.zudin.objectstore;

import org.apache.commons.codec.Charsets;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.lang.StringUtils;

import java.io.*;
import java.util.*;
import java.util.regex.Pattern;

import static org.apache.commons.lang.StringUtils.isNotBlank;

/**
 * @author sergey
 * @since 07.05.18
 */
public class FileSystemObjectStore implements AppendOnlyObjectStore {

    private static final String DIVISOR = " ";

    private final String folder;
    private Map<String, String> index;
    private List<String> batches;

    public FileSystemObjectStore(String folder) {
        this(folder, 4);
    }

    public FileSystemObjectStore(String folder, int batchSize) {
        //todo: check batch size
        if (!folder.endsWith("/")) {
            folder += "/";
        }
        this.folder = folder;
        this.index = new HashMap<>();
        this.batches = new ArrayList<>();
        scan();
        int i = 0;
        while (batches.size() < batchSize) {
            String batch;
            while (true) {
                batch = "batch-" + i++ + ".fsos";
                if (!batches.contains(batch)) {
                    break;
                }
            }
            batches.add(batch);
        }
    }

    public String put(Serializable object) throws IOException {
        String guid = generateGuid();
        //todo: big files?
        String serializedValue = encodeValue(object);
        String batch = getBatch(guid);
        File file = getFile(batch);
        if (!file.exists()) {
            if (!file.createNewFile()) {
                throw new IOException("Cannot create file in folder '" + folder + "'");
            }
        }
        PrintWriter writer = new PrintWriter(new FileOutputStream(file, true));
        writer.println(guid + DIVISOR + serializedValue.length());
        writer.println(serializedValue);
        writer.flush();
        writer.close();
        index.put(guid, batch);
        return guid;
    }

    public Optional<Object> get(String guid) throws IOException {
        String batch = index.get(guid);
        if (batch == null) {
            return Optional.empty();
        }
        try {
            ObjectStoreIterator iterator = new ObjectStoreIterator(getFile(batch));
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

    public void delete(String guid) {
        throw new UnsupportedOperationException();
    }

    private String encodeValue(Serializable object) {
        return Base64.getEncoder().encodeToString(SerializationUtils.serialize(object));
    }

    private Object decodeValue(String value) {
        return SerializationUtils.deserialize(Base64.getDecoder().decode(value));
    }

    private String generateGuid() {
        while (true) {
            String hex = DigestUtils.md5Hex(new Date().toString());
            if (!index.containsKey(hex)) {
                return hex;
            }
        }
    }

    private String getBatch(String guid) {
        return batches.get(Math.abs(guid.hashCode()) % batches.size());
    }

    private File getFile(String batch) {
        return new File(folder + batch);
    }

    private boolean scan() {
        File path = new File(folder);
        if (!path.isDirectory()) {
            throw new IllegalArgumentException("Folder is invalid");
        }
        Pattern pattern = Pattern.compile("batch-\\d+\\.fsos");
        File[] files = path.listFiles(pathname -> pattern.matcher(pathname.getName()).matches());
        if (files.length == 0) {
            return false;
        } else {
            for (File file : files) {
                fillIndex(file);
                batches.add(file.getName());
            }
            return true;
        }
    }


    private void fillIndex(File file) {
        ObjectStoreIterator iterator = new ObjectStoreIterator(file);
        while (iterator.hasNext()) {
            String guid = iterator.next();
            index.put(guid, file.getName());
        }
    }

}
