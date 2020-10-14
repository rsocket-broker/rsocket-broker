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

package io.rsocket.routing.broker.spring;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

import io.rsocket.routing.common.Id;
import io.rsocket.routing.common.spring.TransportProperties;

// TODO: does the broker reuse client properties?
public class BrokerProperties extends TransportProperties {

	public static final String DEFAULT_LOAD_BALANCER = "roundrobin";
	/**
	 * Broker Id.
	 */
	private Id brokerId = Id.random();

	private String defaultLoadBalancer = DEFAULT_LOAD_BALANCER;

	private List<Broker> brokers = new ArrayList<>();

	public BrokerProperties() {
		getTcp().setPort(8001);
	}

	public Id getBrokerId() {
		return this.brokerId;
	}

	public void setBrokerId(Id brokerId) {
		this.brokerId = brokerId;
	}

	public String getDefaultLoadBalancer() {
		return this.defaultLoadBalancer;
	}

	public void setDefaultLoadBalancer(String defaultLoadBalancer) {
		this.defaultLoadBalancer = defaultLoadBalancer;
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
				.add("defaultLoadBalancer=" + defaultLoadBalancer)
				.add("brokers=" + brokers)
				.add("custom=" + getCustom())
				.add("tcp=" + getTcp())
				.add("websocket=" + getWebsocket())
				.toString();
	}

	public static class Broker {
		private final TransportProperties cluster = new TransportProperties();
		private final TransportProperties proxy = new TransportProperties();

		public TransportProperties getCluster() {
			return this.cluster;
		}

		public TransportProperties getProxy() {
			return this.proxy;
		}

		@Override
		public String toString() {
			return new StringJoiner(", ", Broker.class.getSimpleName() + "[", "]")
					.add("cluster=" + cluster)
					.add("proxy=" + proxy)
					.toString();
		}
	}
}
