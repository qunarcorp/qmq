package qunar.tc.qmq.meta.spi;

import java.util.ServiceLoader;

import qunar.tc.qmq.meta.spi.impl.DefaultClientRegisterAuthService;
import qunar.tc.qmq.meta.spi.pojo.ClientRegisterAuthInfo;


/**
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

    public static boolean auth(ClientRegisterAuthInfo authInfo) {
        return INSTANCE.auth(authInfo);
    }


}
