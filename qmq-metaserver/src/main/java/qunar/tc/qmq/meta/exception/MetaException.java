package qunar.tc.qmq.meta.exception;

import org.springframework.util.ObjectUtils;

/**
 * @description：
 * @author     ：zhixin.zhang
 * @date       ：Created in 下午9:04 2021/12/30
 */
public abstract class MetaException extends RuntimeException{

	/** Constructs a new runtime exception with the specified detail message.
	 * The cause is not initialized, and may subsequently be initialized by a
	 * call to {@link #initCause}.
	 *
	 * @param   message   the detail message. The detail message is saved for
	 *          later retrieval by the {@link #getMessage()} method.
	 */
	public MetaException(String message) {
		super(message);
	}

	public MetaException(String message, Throwable cause) {
		super(message, cause);
	}

	@Override
	public boolean equals(Object other) {
		if (this == other) {
			return true;
		}
		if (!(other instanceof MetaException)) {
			return false;
		}
		MetaException otherBe = (MetaException) other;
		return (getMessage().equals(otherBe.getMessage()) &&
				ObjectUtils.nullSafeEquals(getCause(), otherBe.getCause()));
	}

	@Override
	public int hashCode() {
		return getMessage().hashCode();
	}
}
