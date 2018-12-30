package qunar.tc.qmq.delay.store.log;

import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;

/**
 * Created by zhaohui.yu
 * 12/30/18
 */
public class DirectBufCloser {
    public static void close(ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0) {
            return;
        }

        try {
            final Cleaner cleaner = ((DirectBuffer) buffer).cleaner();
            cleaner.clean();
        } catch (Exception ignore) {

        }
    }
}
