package qunar.tc.qmq.store;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Map;
import org.junit.Test;
import org.mockito.Mockito;
import org.powermock.reflect.Whitebox;
import qunar.tc.qmq.TagType;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.base.BaseMessage.keys;
import qunar.tc.qmq.protocol.consumer.PullRequest;
import qunar.tc.qmq.protocol.consumer.SubEnvIsolationPullFilter;
import qunar.tc.qmq.protocol.consumer.TagPullFilter;
import qunar.tc.qmq.store.buffer.Buffer;

/**
 * @author zhenwei.liu
 * @since 2019-08-01
 */
public class TagPullMessageFilterTest {

	/**
	 * 测试 filter 为空的情况
	 */
	@Test
	public void testInactiveOnFilterEmpty() throws Exception {
		Buffer buffer = TestToolBox.createDefaultBuffer();
		PullRequest request = TestToolBox.createDefaultPullRequest();
		TagPullMessageFilter filter = Mockito.spy(TagPullMessageFilter.class);
		boolean active = filter.isActive(request, buffer);
		assertFalse(active);
	}

	/**
	 * 测试 filter 存在且为 no_tag 的情况
	 */
	@Test
	public void testInactiveOnNoTag() throws Exception {
		Buffer buffer = TestToolBox.createDefaultBuffer();
		PullRequest request = TestToolBox.createDefaultPullRequest();
		request.setFilters(Lists.newArrayList(new TagPullFilter(TagType.NO_TAG, Sets.newHashSet())));
		TagPullMessageFilter filter = Mockito.spy(TagPullMessageFilter.class);
		boolean active = filter.isActive(request, buffer);
		assertFalse(active);
	}

	/**
	 * 测试 filter 存在且存在 tag 的情况
	 */
	@Test
	public void testActiveOnTagExists() throws Exception {
		Buffer buffer = TestToolBox.createDefaultBuffer();
		PullRequest request = TestToolBox.createDefaultPullRequest();
		request.setFilters(Lists.newArrayList(new TagPullFilter(TagType.AND, Sets.newHashSet("tag1", "tag2"))));
		TagPullMessageFilter filter = Mockito.spy(TagPullMessageFilter.class);
		boolean active = filter.isActive(request, buffer);
		assertTrue(active);
	}

	/**
	 * 测试请求 or tag 与消息 tag 一致的情况
	 */
	@Test
	public void testOrTagMatches() throws Exception {
		String tag1 = "tag1";
		String tag2 = "tag2";
		PullRequest request = TestToolBox.createDefaultPullRequest();
		request.setFilters(Lists.newArrayList(new TagPullFilter(TagType.OR, Sets.newHashSet(tag1, tag2))));
		BaseMessage message = TestToolBox.createDefaultMessage();
		message.addTag(tag1);
		Buffer buffer = TestToolBox.messageToBuffer(message);
		TagPullMessageFilter filter = Mockito.spy(TagPullMessageFilter.class);
		boolean match = filter.match(request, buffer);
		assertTrue(match);
	}

	/**
	 * 测试请求 and tag 与消息 tag 一致的情况
	 */
	@Test
	public void testAndTagMatches() throws Exception {
		String tag1 = "tag1";
		String tag2 = "tag2";
		PullRequest request = TestToolBox.createDefaultPullRequest();
		request.setFilters(Lists.newArrayList(new TagPullFilter(TagType.AND, Sets.newHashSet(tag1, tag2))));
		BaseMessage message = TestToolBox.createDefaultMessage();
		message.addTag(tag1);
		message.addTag(tag2);
		message.addTag("tag3");
		Buffer buffer = TestToolBox.messageToBuffer(message);
		TagPullMessageFilter filter = Mockito.spy(TagPullMessageFilter.class);
		boolean match = filter.match(request, buffer);
		assertTrue(match);
	}

	/**
	 * 测试请求 and tag 与消息 tag 不一致的情况
	 */
	@Test
	public void testAndTagNotMatches() throws Exception {
		String tag1 = "tag1";
		String tag2 = "tag2";
		String tag3 = "tag3";
		PullRequest request = TestToolBox.createDefaultPullRequest();
		request.setFilters(Lists.newArrayList(new TagPullFilter(TagType.AND, Sets.newHashSet(tag1, tag2, tag3))));
		BaseMessage message = TestToolBox.createDefaultMessage();
		message.addTag(tag1);
		message.addTag(tag2);
		Buffer buffer = TestToolBox.messageToBuffer(message);
		TagPullMessageFilter filter = Mockito.spy(TagPullMessageFilter.class);
		boolean match = filter.match(request, buffer);
		assertFalse(match);
	}
}
