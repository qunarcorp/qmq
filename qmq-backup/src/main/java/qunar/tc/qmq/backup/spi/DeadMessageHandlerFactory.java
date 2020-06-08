package qunar.tc.qmq.backup.spi;

import java.util.ServiceLoader;

import qunar.tc.qmq.backup.spi.impl.DefaultDeadMessageHandler;

/**
 * TODO completion javadoc.
 *
 * @author xiao.liang
 * @since 08 June 2020
 */
public class DeadMessageHandlerFactory {

	private static DeadMessageHandler INSTANCE;

	static {
		ServiceLoader<DeadMessageHandler> services = ServiceLoader.load(DeadMessageHandler.class);
		DeadMessageHandler instance = null;
		for (DeadMessageHandler registry : services) {
			instance = registry;
			break;
		}
		if (instance == null) {
			instance = new DefaultDeadMessageHandler();
		}

		INSTANCE = instance;
	}

	public static DeadMessageHandler load() {
		return INSTANCE;
	}

}
