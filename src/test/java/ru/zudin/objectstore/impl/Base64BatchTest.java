package ru.zudin.objectstore.impl;

/**
 * @author sergey
 * @since 09.05.18
 */
public class Base64BatchTest extends AbstractBatchTest {
    @Override
    protected AbstractFileBatch getBatch(String path) {
        return new Base64Batch(path, "test-batch.bsos", 0.33, 1024 * 1024 * 200);
    }
}
