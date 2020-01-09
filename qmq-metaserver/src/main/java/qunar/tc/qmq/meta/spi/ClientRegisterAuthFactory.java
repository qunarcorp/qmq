package qunar.tc.qmq.meta.spi;

import java.util.ServiceLoader;

import qunar.tc.qmq.meta.spi.impl.DefaultClientRegisterAuthService;


/**
 * TODO completion javadoc.
 *
 * @author xiao.liang
 * @since 27 December 2019
 */
public class ClientRegisterAuthFactory {

	private static ClientRegisterAuthService INSTANCE;

	static {
		ServiceLoader<ClientRegisterAuthService> services = ServiceLoader.load(ClientRegisterAuthService.class);
		ClientRegisterAuthService instance = null;
		for (ClientRegisterAuthService registry : services) {
			instance = registry;
			break;
		}
		if (instance == null) {
			instance = new DefaultClientRegisterAuthService();
		}

		INSTANCE = instance;
	}

	public static ClientRegisterAuthService getClientRegisterAuthService() {
		return INSTANCE;
	}


}
