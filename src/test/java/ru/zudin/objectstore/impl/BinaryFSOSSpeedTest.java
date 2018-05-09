package ru.zudin.objectstore.impl;

/**
 * @author sergey
 * @since 09.05.18
 */
public class BinaryFSOSSpeedTest extends AbstractFSOSSpeedTest {
    @Override
    protected FileSystemObjectStore.BatchType getType() {
        return FileSystemObjectStore.BatchType.BINARY;
    }
}
