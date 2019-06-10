package qunar.tc.qmq.protocol.consumer;

/**
 * @author keli.wang
 * @since 2019-01-02
 */
public class SubEnvIsolationPullFilter implements PullFilter {
    private final String env;
    private final String subEnv;

    public SubEnvIsolationPullFilter(final String env, final String subEnv) {
        this.env = env;
        this.subEnv = subEnv;
    }

    public String getEnv() {
        return env;
    }

    public String getSubEnv() {
        return subEnv;
    }

    @Override
    public PullFilterType type() {
        return PullFilterType.SUB_ENV_ISOLATION;
    }
}
