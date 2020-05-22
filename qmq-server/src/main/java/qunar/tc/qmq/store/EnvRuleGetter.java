package qunar.tc.qmq.store;

import java.util.Collection;

/**
 * @author zhenwei.liu
 * @since 2019-08-05
 */
public interface EnvRuleGetter {

	Collection<SubEnvIsolationRule> getRules();
}
