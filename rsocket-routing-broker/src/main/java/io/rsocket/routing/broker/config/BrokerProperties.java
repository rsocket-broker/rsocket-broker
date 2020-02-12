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

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

import io.rsocket.routing.common.Id;

// TODO: does the broker reuse client properties?
public class BrokerProperties {

	/**
	 * Broker Id.
	 */
	private Id brokerId;

	private List<Broker> brokers = new ArrayList<>();

	public Id getBrokerId() {
		return this.brokerId;
	}

	public void setBrokerId(Id brokerId) {
		this.brokerId = brokerId;
	}

	public List<Broker> getBrokers() {
		return this.brokers;
	}

	public void setBrokers(List<Broker> brokers) {
		this.brokers = brokers;
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", BrokerProperties.class
				.getSimpleName() + "[", "]")
				.add("brokerId=" + brokerId)
				.add("brokers=" + brokers)
				.toString();
	}

	public static class Broker {
		private String host;

		private int port;

		public String getHost() {
			return this.host;
		}

		public void setHost(String host) {
			this.host = host;
		}

		public int getPort() {
			return this.port;
		}

		public void setPort(int port) {
			this.port = port;
		}

		@Override
		public String toString() {
			return new StringJoiner(", ", Broker.class.getSimpleName() + "[", "]")
					.add("host='" + host + "'")
					.add("port=" + port)
					.toString();
		}
	}
}
