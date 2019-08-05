package qunar.tc.qmq.store;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.mockito.Mockito.spy;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.protocol.consumer.PullRequest;
import qunar.tc.qmq.store.buffer.Buffer;

/**
 * @author zhenwei.liu
 * @since 2019-08-01
 */
@RunWith(MockitoJUnitRunner.class)
public class PullMessageFilterChainTest {

	private PullMessageFilter inactiveFilter = new PullMessageFilter() {
		@Override
		public boolean isActive(PullRequest request, Buffer message) {
			return false;
		}

		@Override
		public boolean match(PullRequest request, Buffer message) {
			return false;
		}

		@Override
		public void init(DynamicConfig config) {

		}
	};

	private PullMessageFilter activeNotMatchFilter = new PullMessageFilter() {
		@Override
		public boolean isActive(PullRequest request, Buffer message) {
			return true;
		}

		@Override
		public boolean match(PullRequest request, Buffer message) {
			return false;
		}

		@Override
		public void init(DynamicConfig config) {

		}
	};

	private PullMessageFilter activeAndMatchFilter = new PullMessageFilter() {
		@Override
		public boolean isActive(PullRequest request, Buffer message) {
			return true;
		}

		@Override
		public boolean match(PullRequest request, Buffer message) {
			return true;
		}

		@Override
		public void init(DynamicConfig config) {

		}
	};

	@Mock
	private DynamicConfig dynamicConfig;

	private PullMessageFilterChain pullMessageFilterChain;

	@Before
	public void beforeClass() {
		PullMessageFilterChain old = new PullMessageFilterChain(dynamicConfig);
		this.pullMessageFilterChain = spy(old);
	}

	/**
	 * 所有 filter 未启用, 此时消息被保留
	 */
	@Test
	public void testPullOnFiltersInactive() throws Exception {
		Whitebox.setInternalState(pullMessageFilterChain, "filters", Lists.newArrayList(inactiveFilter));
		PullRequest request = TestToolBox.createDefaultPullRequest();
		Buffer buffer = TestToolBox.createDefaultBuffer();
		boolean needKeep = pullMessageFilterChain.needKeep(request, buffer);
		assertTrue(needKeep);
	}

	/**
	 * filter 启用但不匹配, 此时消息被丢弃
	 */
	@Test
	public void testPullOnFilterActiveNotMatch() throws Exception {
		Whitebox.setInternalState(pullMessageFilterChain, "filters", Lists.newArrayList(activeNotMatchFilter));
		PullRequest request = TestToolBox.createDefaultPullRequest();
		Buffer buffer = TestToolBox.createDefaultBuffer();
		boolean needKeep = pullMessageFilterChain.needKeep(request, buffer);
		assertFalse(needKeep);
	}

	/**
	 * filter 启用且匹配, 此时消息被保留
	 */
	@Test
	public void testPullOnFilterActiveAndMatch() throws Exception {
		Whitebox.setInternalState(pullMessageFilterChain, "filters", Lists.newArrayList(activeAndMatchFilter));
		PullRequest request = TestToolBox.createDefaultPullRequest();
		Buffer buffer = TestToolBox.createDefaultBuffer();
		boolean needKeep = pullMessageFilterChain.needKeep(request, buffer);
		assertTrue(needKeep);
	}

	/**
	 * 多个 Filter, 其中有一个启用且不匹配, 消息被丢弃
	 */
	@Test
	public void testPullOnMultiFilterButNotMatchOne() throws Exception {
		Whitebox.setInternalState(pullMessageFilterChain, "filters",
				Lists.newArrayList(activeAndMatchFilter, activeNotMatchFilter));
		PullRequest request = TestToolBox.createDefaultPullRequest();
		Buffer buffer = TestToolBox.createDefaultBuffer();
		boolean needKeep = pullMessageFilterChain.needKeep(request, buffer);
		assertFalse(needKeep);
	}

	/**
	 * 多个 Filter, 所有都启用且匹配, 消息被保留
	 */
	@Test
	public void testPullOnMultiFilterAndMatchAll() throws Exception {
		Whitebox.setInternalState(pullMessageFilterChain, "filters",
				Lists.newArrayList(activeAndMatchFilter, activeAndMatchFilter));
		PullRequest request = TestToolBox.createDefaultPullRequest();
		Buffer buffer = TestToolBox.createDefaultBuffer();
		boolean needKeep = pullMessageFilterChain.needKeep(request, buffer);
		assertTrue(needKeep);
	}

	/**
	 * 多个 Filter, 所有都未启用, 消息被保留
	 */
	@Test
	public void testPullOnMultiFilterAndInactiveAll() throws Exception {
		Whitebox.setInternalState(pullMessageFilterChain, "filters",
				Lists.newArrayList(inactiveFilter, inactiveFilter));
		PullRequest request = TestToolBox.createDefaultPullRequest();
		Buffer buffer = TestToolBox.createDefaultBuffer();
		boolean needKeep = pullMessageFilterChain.needKeep(request, buffer);
		assertTrue(needKeep);
	}
}
