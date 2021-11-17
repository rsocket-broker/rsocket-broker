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

package io.rsocket.broker.spring.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.scheduling.annotation.Scheduled;

public class ClusterMonitor {
	private final Logger logger = LoggerFactory.getLogger(getClass());

	private final BrokerConnections brokerConnections;
	private final ProxyConnections proxyConnections;

	public ClusterMonitor(BrokerConnections brokerConnections, ProxyConnections proxyConnections) {
		this.brokerConnections = brokerConnections;
		this.proxyConnections = proxyConnections;
	}

	// TODO: Configurable
	@Scheduled(fixedRateString = "5000", initialDelayString = "5000")
	public void printConnections() {
		logger.info("== Broker Connections ==");
		brokerConnections.entries().forEach(entry -> logger.info(entry.getBrokerInfo().toString()));
		logger.info("== Proxy Connections ==");
		proxyConnections.entries().forEach(entry -> logger.info(entry.getBrokerInfo().toString()));
	}
}
