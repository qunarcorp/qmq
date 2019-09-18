package qunar.tc.qmq.store;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.concurrent.NamedThreadFactory;
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.protocol.consumer.PullFilterType;
import qunar.tc.qmq.protocol.consumer.PullRequest;
import qunar.tc.qmq.protocol.consumer.SubEnvIsolationPullFilter;
import qunar.tc.qmq.store.buffer.Buffer;
import qunar.tc.qmq.utils.CharsetUtils;
import qunar.tc.qmq.utils.Flags;

/**
 * @author zhenwei.liu
 * @since 2019-08-01
 */
public class EnvPullMessageFilter implements PullMessageFilter {

	private static final Logger LOG = LoggerFactory.getLogger(EnvPullMessageFilter.class);

	private static final Joiner RULE_KEY_JOINER = Joiner.on('_');

	private volatile ImmutableMap<String, SubEnvIsolationRule> ruleMap = ImmutableMap.of();

	private boolean enableSubEnvIsolation;

	@Override
	public boolean isActive(PullRequest request, Buffer message) {
		if (!enableSubEnvIsolation) {
			return false;
		}

		SubEnvIsolationPullFilter filter = getFilter(request, PullFilterType.SUB_ENV_ISOLATION);
		return filter != null;
	}

	@Override
	public boolean match(PullRequest request, Buffer message) {
		return match(getFilter(request, PullFilterType.SUB_ENV_ISOLATION), request, message);
	}

	@Override
	public void init(DynamicConfig config) {
		this.enableSubEnvIsolation = config.getBoolean("sub_env_isolation_filter.enable", false);
		if (this.enableSubEnvIsolation) {
			ScheduledExecutorService reloadExecutor = Executors
					.newSingleThreadScheduledExecutor(new NamedThreadFactory("reload-env-match-rules"));
			EnvRuleGetter envRuleGetter = null;
			String rulesUrl = config.getString("sub_env_isolation_filter.rules_url", null);
			if (rulesUrl != null) {
				envRuleGetter = new RemoteEnvRuleGetter(rulesUrl);
			} else {
				ServiceLoader<EnvRuleGetter> loader = ServiceLoader.load(EnvRuleGetter.class);
				for (EnvRuleGetter ruleGetter : loader) {
					// 取第一个
					envRuleGetter = ruleGetter;
					break;
				}
			}
			if (envRuleGetter == null) {
				throw new IllegalStateException(
						"EnvRuleGetter could not be null, specify a remote url or a SPI service for it");
			} else {
				final EnvRuleGetter envRuleGetter0 = envRuleGetter;
				reloadExecutor.scheduleAtFixedRate(() -> this.reloadRules(envRuleGetter0), 0, 30, TimeUnit.SECONDS);
			}
		}
	}

	private boolean match(final SubEnvIsolationPullFilter filter, final PullRequest request, final Buffer result) {
		final ByteBuffer message = result.getBuffer();
		message.mark();
		try {
			skipUntilBody(message);
			return isEnvMatch(filter, request, readBody(message));
		} catch (Exception e) {
			LOG.error("check env match failed.", e);
			return false;
		} finally {
			message.reset();
		}
	}

	private void reloadRules(EnvRuleGetter ruleGetter) {
		try {
			Collection<SubEnvIsolationRule> rules = ruleGetter.getRules();
			if (rules != null) {
				this.ruleMap = toMap(rules);
			}
		} catch (Throwable t) {
			LOG.error("update evn rules error", t);
		}
	}

	private void skipUntilBody(final ByteBuffer message) {
		byte flag = message.get();

		skip(message, 8 + 8);
		//subject
		skipString(message);
		//message id
		skipString(message);

		if (Flags.hasTags(flag)) {
			skipTags(message);
		}
	}

	private void skipTags(final ByteBuffer message) {
		final byte tagsSize = message.get();
		for (int i = 0; i < tagsSize; i++) {
			skipString(message);
		}
	}

	private void skipString(final ByteBuffer message) {
		final short len = message.getShort();
		skip(message, len);
	}

	private void skip(final ByteBuffer message, final int bytes) {
		message.position(message.position() + bytes);
	}

	private Map<String, String> readBody(final ByteBuffer message) {
		final int bodySize = message.getInt();
		final ByteBuffer bodyBuffer = message.slice();
		bodyBuffer.limit(bodySize);

		final Map<String, String> body = new HashMap<>();
		while (bodyBuffer.hasRemaining()) {
			final String key = readString(bodyBuffer);
			final String value = readString(bodyBuffer);
			body.put(key, value);
		}
		return body;
	}

	private String readString(final ByteBuffer buffer) {
		final short length = buffer.getShort();
		if (length < 0) {
			throw new RuntimeException("message data corrupt");
		}

		if (length == 0) {
			return "";
		}

		final byte[] bytes = new byte[length];
		buffer.get(bytes);
		return new String(bytes, CharsetUtils.UTF8);
	}

	private boolean isEnvMatch(final SubEnvIsolationPullFilter filter, final PullRequest request,
			final Map<String, String> body) {
		final String key = buildRuleKey(filter, request);
		final SubEnvIsolationRule rule = ruleMap.get(key);
		if (rule == null) {
			return false;
		}

		final String env = body.get(BaseMessage.keys.qmq_env.name());
		final String subEnv = body.get(BaseMessage.keys.qmq_subEnv.name());
		if (Strings.isNullOrEmpty(env) || subEnv == null) {
			return false;
		}

		return Objects.equals(rule.getSubjectEnv(), env) && Objects.equals(rule.getSubjectSubEnv(), subEnv);
	}

	private String buildRuleKey(final SubEnvIsolationPullFilter filter, final PullRequest request) {
		if (request.isExclusiveConsume()) {
			return RULE_KEY_JOINER.join(request.getPartitionName(), "", filter.getEnv(), filter.getSubEnv());
		}
		return RULE_KEY_JOINER.join(request.getPartitionName(), request.getGroup());
	}

	private ImmutableMap<String, SubEnvIsolationRule> toMap(final Collection<SubEnvIsolationRule> rules) {
		final ImmutableMap.Builder<String, SubEnvIsolationRule> builder = ImmutableMap.builder();
		for (final SubEnvIsolationRule rule : rules) {
			final String key = RULE_KEY_JOINER
					.join(rule.getSubject(), rule.getGroup(), rule.getGroupEnv(), rule.getGroupSubEnv());
			builder.put(key, rule);
		}
		return builder.build();
	}
}
