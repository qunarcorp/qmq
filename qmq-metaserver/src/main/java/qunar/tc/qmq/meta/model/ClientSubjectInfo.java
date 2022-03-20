package qunar.tc.qmq.meta.model;

import java.io.Serializable;

/**
 * @description：
 * @author     ：zhixin.zhang
 * @date       ：Created in 下午7:39 2022/2/9
 */
public class ClientSubjectInfo implements Serializable {


	private static final long serialVersionUID = -8757348822377575171L;

	private String clientId;
	private String subject;

	public ClientSubjectInfo() {
	}

	public ClientSubjectInfo(String clientId, String subject) {
		this.clientId = clientId;
		this.subject = subject;
	}

	public static ClientSubjectInfo of(String clientId, String subject) {
		return new ClientSubjectInfo(clientId, subject);
	}

	public String getClientId() {
		return clientId;
	}

	public void setClientId(String clientId) {
		this.clientId = clientId;
	}

	public String getSubject() {
		return subject;
	}

	public void setSubject(String subject) {
		this.subject = subject;
	}
}
