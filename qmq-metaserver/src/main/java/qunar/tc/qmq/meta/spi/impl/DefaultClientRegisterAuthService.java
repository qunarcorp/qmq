package qunar.tc.qmq.meta.spi.impl;

import qunar.tc.qmq.meta.spi.ClientRegisterAuthService;
import qunar.tc.qmq.meta.spi.pojo.ClientRegisterAuthInfo;

/**
 * TODO completion javadoc.
 *
 * @author xiao.liang
 * @since 27 December 2019
 */
public class DefaultClientRegisterAuthService implements ClientRegisterAuthService {

	@Override
	public boolean auth(ClientRegisterAuthInfo authInfo) {
		return true;
	}
}
