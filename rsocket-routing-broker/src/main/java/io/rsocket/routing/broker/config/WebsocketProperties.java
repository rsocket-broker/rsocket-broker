/*
 * Copyright 2020 the original author or authors.
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

package io.rsocket.routing.broker.config;

import java.net.URI;
import java.util.StringJoiner;

public class WebsocketProperties extends HostPortProperties {

	/**
	 * Path under which RSocket handles requests (only works with websocket
	 * transport).
	 */
	private String mappingPath;

	public String getMappingPath() {
		return this.mappingPath;
	}

	public void setMappingPath(String mappingPath) {
		this.mappingPath = mappingPath;
	}

	public URI getUri() {
		// TODO: validation, scheme
		return URI.create("ws://" + getHost() + ":" + getPort() + mappingPath);
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", WebsocketProperties.class.getSimpleName() + "[", "]")
				.add("address='" + getHost() + "'")
				.add("port='" + getPort() + "'")
				.add("mappingPath='" + mappingPath + "'")
				.toString();
	}
}
