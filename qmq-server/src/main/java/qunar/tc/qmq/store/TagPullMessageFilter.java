package qunar.tc.qmq.store;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import qunar.tc.qmq.TagType;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.protocol.consumer.PullFilterType;
import qunar.tc.qmq.protocol.consumer.PullRequest;
import qunar.tc.qmq.protocol.consumer.TagPullFilter;
import qunar.tc.qmq.store.buffer.Buffer;
import qunar.tc.qmq.utils.Flags;
import qunar.tc.qmq.utils.ListUtils;

/**
 * @author zhenwei.liu
 * @since 2019-08-01
 */
public class TagPullMessageFilter implements PullMessageFilter {

	@Override
	public boolean isActive(PullRequest request, Buffer message) {
		TagPullFilter filter = getFilter(request, PullFilterType.TAG);
		if (filter == null) {
			return false;
		}

		return !noRequestTag(filter);
	}

	@Override
	@SuppressWarnings("all")
	public boolean match(PullRequest request, Buffer message) {
		TagPullFilter filter = getFilter(request, PullFilterType.TAG);
		return match(message, filter.getTags(), filter.getTagTypeCode());
	}

	@Override
	public void init(DynamicConfig config) {

	}

	private boolean noRequestTag(final TagPullFilter filter) {
		int tagTypeCode = filter.getTagTypeCode();
		if (TagType.NO_TAG.getCode() == tagTypeCode) {
			return true;
		}
		List<byte[]> tags = filter.getTags();
		return tags == null || tags.isEmpty();
	}

	static boolean match(Buffer result, List<byte[]> requestTags, int tagTypeCode) {
		ByteBuffer message = result.getBuffer();
		message.mark();
		byte flag = message.get();
		if (!Flags.hasTags(flag)) {
			message.reset();
			return false;
		}
		skip(message, 8 + 8);
		//subject
		skipString(message);
		//message id
		skipString(message);

		final byte tagsSize = message.get();
		if (tagsSize == 1) {
			final short len = message.getShort();
			final byte[] tag = new byte[len];
			message.get(tag);
			message.reset();
			return matchOneTag(tag, requestTags, tagTypeCode);
		}
		List<byte[]> tags = new ArrayList<>(tagsSize);
		for (int i = 0; i < tagsSize; i++) {
			final short len = message.getShort();
			final byte[] bs = new byte[len];
			message.get(bs);
			tags.add(bs);
		}
		message.reset();
		return matchTags(tags, requestTags, tagTypeCode);
	}

	private static void skipString(ByteBuffer input) {
		final short len = input.getShort();
		skip(input, len);
	}

	private static void skip(ByteBuffer input, int bytes) {
		input.position(input.position() + bytes);
	}

	private static boolean matchOneTag(byte[] tag, List<byte[]> requestTags, int tagTypeCode) {
		if (requestTags.size() == 1 || TagType.OR.getCode() == tagTypeCode) {
			return ListUtils.contains(requestTags, tag);
		}

		if (TagType.AND.getCode() == tagTypeCode && requestTags.size() > 1) {
			return false;
		}

		return false;
	}

	private static boolean matchTags(List<byte[]> messageTags, List<byte[]> requestTags, int tagTypeCode) {
		if (tagTypeCode == TagType.AND.getCode()) {
			return ListUtils.containsAll(messageTags, requestTags);
		}
		if (tagTypeCode == TagType.OR.getCode()) {
			return ListUtils.intersection(messageTags, requestTags);
		}
		return false;
	}
}
