package qunar.tc.qmq.backup.spi;

import java.util.ServiceLoader;

import qunar.tc.qmq.backup.spi.impl.DefaultDeadMessageSpiHandler;

/**
 * TODO completion javadoc.
 *
 * @author xiao.liang
 * @since 08 June 2020
 */
public class DeadMessageHandlerFactory {

	private static DeadMessageSpiHandler INSTANCE;

	static {
		ServiceLoader<DeadMessageSpiHandler> services = ServiceLoader.load(DeadMessageSpiHandler.class);
		DeadMessageSpiHandler instance = null;
		for (DeadMessageSpiHandler registry : services) {
			instance = registry;
			break;
		}
		if (instance == null) {
			instance = new DefaultDeadMessageSpiHandler();
		}

		INSTANCE = instance;
	}

	public static DeadMessageSpiHandler load() {
		return INSTANCE;
	}

}
