package qunar.tc.qmq.consumer.pull;

import qunar.tc.qmq.consumer.register.RegistParam;

/**
 * TODO completion javadoc.
 *
 * @author xiao.liang
 * @since 14 January 2020
 */
public class PushConsumerParam {

	private String subject;
	private String group;
	private RegistParam registParam;

	public PushConsumerParam(String subject, String group, RegistParam registParam) {
		this.subject = subject;
		this.group = group;
		this.registParam = registParam;
	}

	public String getSubject() {
		return subject;
	}

	public void setSubject(String subject) {
		this.subject = subject;
	}

	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	public RegistParam getRegistParam() {
		return registParam;
	}

	public void setRegistParam(RegistParam registParam) {
		this.registParam = registParam;
	}
}
