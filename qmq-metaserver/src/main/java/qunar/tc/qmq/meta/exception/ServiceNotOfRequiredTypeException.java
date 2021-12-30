package qunar.tc.qmq.meta.exception;

import org.springframework.util.ClassUtils;

/**
 * @description：
 * @author     ：zhixin.zhang
 * @date       ：Created in 下午9:13 2021/12/30
 */
public class ServiceNotOfRequiredTypeException extends MetaException{
	private String beanName;
	private Class<?> requiredType;
	private Class<?> actualType;

	public ServiceNotOfRequiredTypeException(String name, Class<?> requiredType, Class<?> actualType) {
		super("Service named '" + name + "' is expected to be of type '" + ClassUtils.getQualifiedName(requiredType) + "' but was actually of type '" + ClassUtils.getQualifiedName(actualType) + "'");
		this.beanName = name;
		this.requiredType = requiredType;
		this.actualType = actualType;
	}

	public String getBeanName() {
		return this.beanName;
	}

	public Class<?> getRequiredType() {
		return this.requiredType;
	}

	public Class<?> getActualType() {
		return this.actualType;
	}
}
