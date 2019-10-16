package qunar.tc.qmq.test.client;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import qunar.tc.qmq.common.ClientIdProvider;

/**
 * @author zhenwei.liu
 * @since 2019-10-11
 */
public class TestClientIdProvider implements ClientIdProvider {

    @Override
    public String get() {
        String id = MessageTestUtils.getClientId();
        Preconditions.checkArgument(!Strings.isNullOrEmpty(id));
        return id;
    }
}
