/*
 * Copyright 2020-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package io.rsocket.routing.http.bridge.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

import static io.rsocket.routing.http.bridge.config.RSocketHttpBridgeProperties.BRIDGE_CONFIG_PREFIX;

/**
 * @author Olga Maciaszek-Sharma
 */
@ConfigurationProperties(BRIDGE_CONFIG_PREFIX)
public class RSocketHttpBridgeProperties {

	public static final String BRIDGE_CONFIG_PREFIX = "io.rsocket.routing.http.bridge";

	private boolean requestResponseDefault = true;

	private String tagsHeaderName = "X-RSocket-Tags";

	private String brokerDataHeaderName = "X-RSocket-Broker";

	private Duration timeout = Duration.ofSeconds(30);

	public boolean isRequestResponseDefault() {
		return requestResponseDefault;
	}

	public void setRequestResponseDefault(boolean requestResponseDefault) {
		this.requestResponseDefault = requestResponseDefault;
	}

	public String getTagsHeaderName() {
		return tagsHeaderName;
	}

	public void setTagsHeaderName(String tagsHeaderName) {
		this.tagsHeaderName = tagsHeaderName;
	}

	public String getBrokerDataHeaderName() {
		return brokerDataHeaderName;
	}

	public void setBrokerDataHeaderName(String brokerDataHeaderName) {
		this.brokerDataHeaderName = brokerDataHeaderName;
	}

	public Duration getTimeout() {
		return timeout;
	}

	public void setTimeout(Duration timeout) {
		this.timeout = timeout;
	}
}
