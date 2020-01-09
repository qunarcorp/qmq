package qunar.tc.qmq.meta.spi;

import qunar.tc.qmq.meta.spi.pojo.ClientRegisterAuthInfo;

/**
 * TODO completion javadoc.
 *
 * @author xiao.liang
 * @since 27 December 2019
 */
public interface ClientRegisterAuthService {

	boolean auth(ClientRegisterAuthInfo authInfo);

}
