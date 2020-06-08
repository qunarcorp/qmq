package qunar.tc.qmq.backup.spi;

import qunar.tc.qmq.store.MessageQueryIndex;

/**
 * TODO completion javadoc.
 *
 * @author xiao.liang
 * @since 08 June 2020
 */
public interface DeadMessageHandler {

	void handle(MessageQueryIndex index);

}
