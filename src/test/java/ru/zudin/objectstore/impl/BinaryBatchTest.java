package ru.zudin.objectstore.impl;

/**
 * @author sergey
 * @since 09.05.18
 */
public class BinaryBatchTest extends AbstractBatchTest {
    @Override
    protected AbstractFileBatch getBatch(String path) {
        return new BinaryBatch(path, "test-batch.bnos", 0.33, 1024 * 1024 * 200);
    }
}
