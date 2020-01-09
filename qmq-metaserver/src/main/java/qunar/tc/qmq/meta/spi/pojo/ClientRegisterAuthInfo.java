package qunar.tc.qmq.meta.spi.pojo;

import qunar.tc.qmq.common.ClientType;

/**
 * TODO completion javadoc.
 *
 * @author xiao.liang
 * @since 27 December 2019
 */
public class ClientRegisterAuthInfo {

	private String subject;
	private String consumerGroup;
	private String clientId;
	private String appCode;
	private ClientType clientType;

	public String getSubject() {
		return subject;
	}

	public void setSubject(String subject) {
		this.subject = subject;
	}

	public String getConsumerGroup() {
		return consumerGroup;
	}

	public void setConsumerGroup(String consumerGroup) {
		this.consumerGroup = consumerGroup;
	}

	public String getClientId() {
		return clientId;
	}

	public void setClientId(String clientId) {
		this.clientId = clientId;
	}

	public String getAppCode() {
		return appCode;
	}

	public void setAppCode(String appCode) {
		this.appCode = appCode;
	}

	public ClientType getClientType() {
		return clientType;
	}

	public void setClientType(ClientType clientType) {
		this.clientType = clientType;
	}
}
