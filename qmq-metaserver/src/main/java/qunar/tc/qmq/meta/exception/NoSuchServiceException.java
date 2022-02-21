package qunar.tc.qmq.meta.exception;

/**
 * @description：
 * @author     ：zhixin.zhang
 * @date       ：Created in 下午9:06 2021/12/30
 */
public class NoSuchServiceException extends MetaException{

	private String serviceName;

	public NoSuchServiceException(String name) {
		super("No Service named '" + name + "' available");
		this.serviceName = name;
	}

	public NoSuchServiceException(String name, String message) {
		super("No Service named '" + name + "' available: " + message);
		this.serviceName = name;
	}
}
