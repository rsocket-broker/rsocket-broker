/*
 * Copyright 2021 the original author or authors.
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

package io.rsocket.broker.spring;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

import io.rsocket.broker.common.Id;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.Errors;
import org.springframework.validation.ValidationUtils;
import org.springframework.validation.Validator;
import org.springframework.validation.annotation.Validated;

// TODO: does the broker reuse client properties?
@Validated
@ConfigurationProperties(BrokerProperties.PREFIX)
public class BrokerProperties implements Validator {
	public static final String PREFIX = "io.rsocket.broker";

	public static final String ROUND_ROBIN_LOAD_BALANCER_NAME = "roundrobin";
	public static final String WEIGHTED_BALANCER_NAME = "weighted";
	/**
	 * Broker Id.
	 */
	private Id brokerId = Id.random();

	private URI uri = URI.create("tcp://localhost:8001");

	//TODO: flag send to client

	private String defaultLoadBalancer = ROUND_ROBIN_LOAD_BALANCER_NAME;

	private Cluster cluster = new Cluster();

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

	public Cluster getCluster() {
		return this.cluster;
	}

	public void setCluster(Cluster cluster) {
		this.cluster = cluster;
	}

	public List<Broker> getBrokers() {
		return this.brokers;
	}

	public void setBrokers(List<Broker> brokers) {
		this.brokers = brokers;
	}

	@Override
	public boolean supports(Class<?> clazz) {
		return BrokerProperties.class.isAssignableFrom(clazz);
	}

	@Override
	public void validate(Object target, Errors errors) {
		ValidationUtils.rejectIfEmptyOrWhitespace(errors, "uri", "field.required");

		BrokerProperties properties = (BrokerProperties) target;
		if (properties.getCluster().isEnabled() && properties.getCluster().getUri() == null) {
			errors.rejectValue("cluster.uri", "field.required", Cluster.PREFIX + ".uri may not be null if cluster is enabled");
		}
	}

	@Override
	public String toString() {
		return new StringJoiner(", ", BrokerProperties.class
				.getSimpleName() + "[", "]")
				.add("brokerId=" + brokerId)
				.add("uri=" + uri)
				.add("defaultLoadBalancer=" + defaultLoadBalancer)
				.add("cluster=" + cluster)
				.add("brokers=" + brokers)
				.toString();
	}

	public static class Cluster {
		public static final String PREFIX = BrokerProperties.PREFIX + ".cluster";

		private boolean enabled = true;

		private URI uri = URI.create("tcp://localhost:7001");

		public boolean isEnabled() {
			return this.enabled;
		}

		public void setEnabled(boolean enabled) {
			this.enabled = enabled;
		}

		public URI getUri() {
			return this.uri;
		}

		public void setUri(URI uri) {
			this.uri = uri;
		}

		@Override
		public String toString() {
			return new StringJoiner(", ", Cluster.class.getSimpleName() + "[", "]")
					.add("enabled=" + enabled)
					.add("uri=" + uri)
					.toString();
		}
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
