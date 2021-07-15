package io.rsocket.routing.http.bridge.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import static io.rsocket.routing.http.bridge.config.RSocketHttpBridgeProperties.BRIDGE_CONFIG_PREFIX;

/**
 * @author Olga Maciaszek-Sharma
 */
@ConfigurationProperties(BRIDGE_CONFIG_PREFIX)
public class RSocketHttpBridgeProperties {

	public static final String BRIDGE_CONFIG_PREFIX = "io.rsocket.routing.http.bridge";

	private boolean requestResponseDefault = true;

	private String tagHeaderName = "X-RSocket-Tags";

	public boolean isRequestResponseDefault() {
		return requestResponseDefault;
	}

	public void setRequestResponseDefault(boolean requestResponseDefault) {
		this.requestResponseDefault = requestResponseDefault;
	}

	public String getTagHeaderName() {
		return tagHeaderName;
	}

	public void setTagHeaderName(String tagHeaderName) {
		this.tagHeaderName = tagHeaderName;
	}
}
