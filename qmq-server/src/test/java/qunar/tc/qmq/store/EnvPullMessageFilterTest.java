package qunar.tc.qmq.store;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Map;
import org.junit.Test;
import org.mockito.Mockito;
import org.powermock.reflect.Whitebox;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.base.BaseMessage.keys;
import qunar.tc.qmq.protocol.consumer.PullRequest;
import qunar.tc.qmq.protocol.consumer.SubEnvIsolationPullFilter;
import qunar.tc.qmq.store.buffer.Buffer;

/**
 * @author zhenwei.liu
 * @since 2019-08-01
 */
public class EnvPullMessageFilterTest {

	/**
	 * 测试开关关闭时启用状态为关闭
	 */
	@Test
	public void testInactiveOnSwitchOff() throws Exception {
		Buffer buffer = TestToolBox.createDefaultBuffer();
		PullRequest request = TestToolBox.createDefaultPullRequest();
		EnvPullMessageFilter filter = Mockito.spy(EnvPullMessageFilter.class);
		Whitebox.setInternalState(filter, "enableSubEnvIsolation", false);
		boolean active = filter.isActive(request, buffer);
		assertFalse(active);
	}

	/**
	 * 测试开关打开但 filter 为空的情况
	 */
	@Test
	public void testInactiveOnSwitchOnButFilterEmpty() {
		Buffer buffer = TestToolBox.createDefaultBuffer();
		PullRequest request = TestToolBox.createDefaultPullRequest();
		EnvPullMessageFilter filter = Mockito.spy(EnvPullMessageFilter.class);
		Whitebox.setInternalState(filter, "enableSubEnvIsolation", true);
		boolean active = filter.isActive(request, buffer);
		assertFalse(active);
	}

	/**
	 * 测试开关打开但 filter 存在的情况
	 */
	@Test
	public void testInactiveOnSwitchOnButFilterExists() {
		Buffer buffer = TestToolBox.createDefaultBuffer();
		PullRequest request = TestToolBox.createDefaultPullRequest();
		request.setFilters(Lists.newArrayList(new SubEnvIsolationPullFilter("test", "test")));
		EnvPullMessageFilter filter = Mockito.spy(EnvPullMessageFilter.class);
		Whitebox.setInternalState(filter, "enableSubEnvIsolation", true);
		boolean active = filter.isActive(request, buffer);
		assertTrue(active);
	}

	/**
	 * 测试消息 tag 与 rule 匹配的情况
	 */
	@Test
	public void testMatchOnMessageTagEqualsRuleTag() {
		String env = "test";
		String subEnv = "testSub";
		String subject = TestToolBox.DEFAULT_SUBJECT;
		String group = TestToolBox.DEFAULT_GROUP;

		BaseMessage message = TestToolBox.createDefaultMessage();
		message.setProperty(keys.qmq_env, env);
		message.setProperty(keys.qmq_subEnv, subEnv);
		Buffer buffer = TestToolBox.messageToBuffer(message);
		PullRequest request = TestToolBox.createDefaultPullRequest();
		request.setFilters(Lists.newArrayList(new SubEnvIsolationPullFilter(env, subEnv)));
		EnvPullMessageFilter filter = Mockito.spy(EnvPullMessageFilter.class);
		Map<String, SubEnvIsolationRule> ruleMap = Maps.newHashMap();
		SubEnvIsolationRule rule = new SubEnvIsolationRule();
		rule.setSubject(subject);
		rule.setGroup(group);
		rule.setSubjectEnv(env);
		rule.setGroupEnv(env);
		rule.setSubjectSubEnv(subEnv);
		rule.setGroupSubEnv(subEnv);
		ruleMap.put(subject + "_" + group, rule);
		Whitebox.setInternalState(filter, "enableSubEnvIsolation", true);
		Whitebox.setInternalState(filter, "ruleMap", ImmutableMap.copyOf(ruleMap));

		boolean match = filter.match(request, buffer);
		assertTrue(match);
	}

	/**
	 * 测试消息 tag 与 rule 不匹配的情况
	 */
	@Test
	public void testMatchOnMessageTagNotEqualsRuleTag() {
		String env = "test";
		String subEnv = "testSub";

		String msgEnv = "msgTest";
		String msgSubEnv = "msgTestSub";

		String subject = TestToolBox.DEFAULT_SUBJECT;
		String group = TestToolBox.DEFAULT_GROUP;

		BaseMessage message = TestToolBox.createDefaultMessage();
		message.setProperty(keys.qmq_env, msgEnv);
		message.setProperty(keys.qmq_subEnv, msgSubEnv);
		Buffer buffer = TestToolBox.messageToBuffer(message);
		PullRequest request = TestToolBox.createDefaultPullRequest();
		request.setFilters(Lists.newArrayList(new SubEnvIsolationPullFilter(env, subEnv)));
		EnvPullMessageFilter filter = Mockito.spy(EnvPullMessageFilter.class);
		Map<String, SubEnvIsolationRule> ruleMap = Maps.newHashMap();
		SubEnvIsolationRule rule = new SubEnvIsolationRule();
		rule.setSubject(subject);
		rule.setGroup(group);
		rule.setSubjectEnv(env);
		rule.setGroupEnv(env);
		rule.setSubjectSubEnv(subEnv);
		rule.setGroupSubEnv(subEnv);
		ruleMap.put(subject + "_" + group, rule);
		Whitebox.setInternalState(filter, "enableSubEnvIsolation", true);
		Whitebox.setInternalState(filter, "ruleMap", ImmutableMap.copyOf(ruleMap));

		boolean match = filter.match(request, buffer);
		assertFalse(match);
	}

}
