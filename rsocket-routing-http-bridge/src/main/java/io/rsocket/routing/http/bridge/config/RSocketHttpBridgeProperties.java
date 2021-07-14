package io.rsocket.routing.http.bridge.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author Olga Maciaszek-Sharma
 */
@ConfigurationProperties("io.rsocket.routing.http.bridge")
public class RSocketHttpBridgeProperties {

	private boolean requestResponseDefault = true;

	public boolean isRequestResponseDefault() {
		return requestResponseDefault;
	}

	public void setRequestResponseDefault(boolean requestResponseDefault) {
		this.requestResponseDefault = requestResponseDefault;
	}
}
