package qunar.tc.qmq.store.buffer;

import java.nio.ByteBuffer;

/**
 * @author keli.wang
 * @since 2019-02-19
 */
public interface Buffer {
    ByteBuffer getBuffer();

    int getSize();

    boolean retain();

    boolean release();
}
