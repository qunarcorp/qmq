package qunar.tc.qmq.store;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.common.JsonHolder;

/**
 * @author zhenwei.liu
 * @since 2019-08-05
 */
public class RemoteEnvRuleGetter implements EnvRuleGetter {

	private static final Logger LOG = LoggerFactory.getLogger(RemoteEnvRuleGetter.class);
	private static final ObjectMapper objectMapper = JsonHolder.getMapper();
	private static final int DEF_RETRY = 3;

	private String url;

	public RemoteEnvRuleGetter(String url) {
		this.url = url;
	}

	@Override
	public Collection<SubEnvIsolationRule> getRules() {
		return reloadWithRetry(url, DEF_RETRY);
	}

	private Collection<SubEnvIsolationRule> reloadWithRetry(final String rulesUrl, final int retryCount) {
		for (int i = 0; i < retryCount; i++) {
			try {
				return reloadRules(rulesUrl);
			} catch (Exception e) {
				LOG.error("unknown exception caught.", e);
			}
		}
		LOG.info("reload env match rules failed after retry {} times. url: {}", retryCount, rulesUrl);
		return null;
	}

	private Collection<SubEnvIsolationRule> reloadRules(final String rulesUrl) {
		try {
			final String data = request(rulesUrl);
			try {
				return objectMapper.readValue(data, new TypeReference<List<SubEnvIsolationRule>>() {
				});
			} catch (Exception e) {
				LOG.error("Failed to deserialize env isolation rules.", e);
				return null;
			}
		} catch (Exception e) {
			LOG.error("reload env match rules failed. url: {}", rulesUrl, e);
			return null;
		}
	}

	private String request(final String httpUrl) throws IOException {
		final URL url = new URL(httpUrl);
		final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setRequestMethod("GET");
		conn.setConnectTimeout(5000);
		conn.setReadTimeout(5000);

		try (final BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
			final StringBuilder sb = new StringBuilder();
			while (true) {
				final String line = reader.readLine();
				if (line == null) {
					break;
				}
				sb.append(line);
			}
			return sb.toString();
		} finally {
			conn.disconnect();
		}
	}
}
