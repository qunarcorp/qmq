package qunar.tc.qmq.backup.util;

import com.google.common.collect.Sets;
import qunar.tc.qmq.utils.Flags;
import qunar.tc.qmq.utils.PayloadHolderUtils;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Set;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-11-29 10:40
 */
public class Tags {

    public static Set<String> readTags(byte flag, ByteBuffer buffer) {
        //noinspection Duplicates
        if (Flags.hasTags(flag)) {
            final byte tagsSize = buffer.get();
            Set<String> tags = Sets.newHashSetWithExpectedSize(tagsSize);
            for (int i = 0; i < tagsSize; i++) {
                final String tag = PayloadHolderUtils.readString(buffer);
                tags.add(tag);
            }
            return tags;

        }
        return Collections.emptySet();
    }
}
