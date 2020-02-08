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

import java.net.InetAddress;
import java.util.StringJoiner;

public abstract class AbstractBrokerProperties {

	/**
	 * Server port.
	 */
	private Integer port;

	/**
	 * Network address to which the server should bind.
	 */
	private InetAddress address;

	/**
	 * RSocket transport protocol.
	 */
	private final Transport transport;

	/**
	 * Path under which RSocket handles requests (only works with websocket
	 * transport).
	 */
	private String mappingPath;

	private String type;

	public AbstractBrokerProperties(Transport transport) {
		this.transport = transport;
	}

	public Integer getPort() {
		return this.port;
	}

	public void setPort(Integer port) {
		this.port = port;
	}

	public InetAddress getAddress() {
		return this.address;
	}

	public void setAddress(InetAddress address) {
		this.address = address;
	}

	public Transport getTransport() {
		return this.transport;
	}

	public String getMappingPath() {
		return this.mappingPath;
	}

	public void setMappingPath(String mappingPath) {
		this.mappingPath = mappingPath;
	}

	public String getType() {
		return this.type;
	}

	public void setType(String type) {
		this.type = type;
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", getClass()
				.getSimpleName() + "[", "]")
				.add("port=" + port)
				.add("address=" + address)
				.add("transport=" + transport)
				.add("mappingPath='" + mappingPath + "'")
				.add("type='" + type + "'")
				.toString();
	}

	/**
	 * Choice of transport protocol for the RSocket server.
	 */
	public enum Transport {

		/**
		 * TCP transport protocol.
		 */
		TCP,

		/**
		 * WebSocket transport protocol.
		 */
		WEBSOCKET

	}
}
