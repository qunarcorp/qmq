package qunar.tc.qmq.common;

import java.io.Serializable;
import java.util.Map;

import com.google.common.collect.Maps;

/**
 * @description：
 * @author     ：zhixin.zhang
 * @date       ：Created in 下午9:04 2021/12/14
 */
public class ClientInfo implements Serializable {


	private static final long serialVersionUID = -4540965581855269195L;

	/**
	 * 应用名
	 */
	private String appCode;
	/**
	 * 逻辑机房
	 */
	private String ldc;
	/**
	 * 扩展
	 */
	private Map<String,String> ext;


	public ClientInfo(String appCode, String ldc, Map<String, String> ext) {
		this.appCode = appCode;
		this.ldc = ldc;
		this.ext = ext;
	}

	public static ClientInfo of(String appCode, String ldc, Map<String, String> ext) {
		return new ClientInfo(appCode, ldc, ext);
	}

	public static ClientInfo of(String appCode, String ldc) {
		return new ClientInfo(appCode, ldc, Maps.newHashMap());
	}
	public String getAppCode() {
		return appCode;
	}

	public void setAppCode(String appCode) {
		this.appCode = appCode;
	}

	public String getLdc() {
		return ldc;
	}

	public void setLdc(String ldc) {
		this.ldc = ldc;
	}

	public Map<String, String> getExt() {
		return ext;
	}

	public void setExt(Map<String, String> ext) {
		this.ext = ext;
	}
}
