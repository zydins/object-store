package ru.zudin.objectstore;

import java.io.*;

/**
 * @author Sergey Zudin
 * @since 08.05.18.
 */
class Batch implements Closeable {

    private final String path;
    private final String name;
    private PrintWriter printWriter;

    public Batch(String path, String name) {
        this.path = path;
        this.name = name;
        this.printWriter = null;
    }

    public String getName() {
        return name;
    }

    public PrintWriter getPrintWriter() throws IOException {
        if (printWriter == null) {
            printWriter = createPrintWriter();
        }
        return printWriter;
    }

    public BatchIterator createIterator() {
        return new BatchIterator(getFile());
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
}
