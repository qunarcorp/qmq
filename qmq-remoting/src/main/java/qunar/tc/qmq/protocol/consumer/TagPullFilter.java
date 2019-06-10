package qunar.tc.qmq.protocol.consumer;

import qunar.tc.qmq.TagType;
import qunar.tc.qmq.utils.CharsetUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author keli.wang
 * @since 2019-01-02
 */
public class TagPullFilter implements PullFilter {
    private final int tagTypeCode;
    private final List<byte[]> tags;

    public TagPullFilter(final int tagTypeCode, final List<byte[]> tags) {
        this.tagTypeCode = tagTypeCode;
        this.tags = tags;
    }

    public TagPullFilter(final TagType tagType, final Set<String> tags) {
        this.tagTypeCode = tagType.getCode();
        this.tags = toBytes(tags);
    }

    private List<byte[]> toBytes(final Set<String> tags) {
        final List<byte[]> bytesTags = new ArrayList<>(tags.size());
        for (final String tag : tags) {
            bytesTags.add(tag.getBytes(CharsetUtils.UTF8));
        }
        return bytesTags;
    }

    public int getTagTypeCode() {
        return tagTypeCode;
    }

    public List<byte[]> getTags() {
        return tags;
    }

    @Override
    public PullFilterType type() {
        return PullFilterType.TAG;
    }
}
