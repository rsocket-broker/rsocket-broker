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

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

import io.rsocket.routing.common.Id;

// TODO: does the broker reuse client properties?
public class BrokerProperties {

	public static final String ROUND_ROBIN_LOAD_BALANCER_NAME = "roundrobin";
	public static final String WEIGHTED_BALANCER_NAME = "weighted";
	/**
	 * Broker Id.
	 */
	private Id brokerId = Id.random();

	private URI uri = URI.create("tcp://localhost:8001");

	//TODO: flag send to client

	private String defaultLoadBalancer = ROUND_ROBIN_LOAD_BALANCER_NAME;

	private List<Broker> brokers = new ArrayList<>();

	public Id getBrokerId() {
		return this.brokerId;
	}

	public void setBrokerId(Id brokerId) {
		this.brokerId = brokerId;
	}

	public URI getUri() {
		return this.uri;
	}

	public void setUri(URI uri) {
		this.uri = uri;
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
				.add("uri=" + uri)
				.add("defaultLoadBalancer=" + defaultLoadBalancer)
				.add("brokers=" + brokers)
				.toString();
	}

	public static class Broker {
		private URI cluster;
		private URI proxy;

		public URI getCluster() {
			return this.cluster;
		}

		public void setCluster(URI cluster) {
			this.cluster = cluster;
		}

		public URI getProxy() {
			return this.proxy;
		}

		public void setProxy(URI proxy) {
			this.proxy = proxy;
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
