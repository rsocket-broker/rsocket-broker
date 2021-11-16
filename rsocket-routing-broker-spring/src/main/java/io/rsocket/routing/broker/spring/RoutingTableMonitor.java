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

package io.rsocket.routing.broker.spring;

import io.rsocket.routing.broker.RoutingTable;
import io.rsocket.routing.common.Tags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.scheduling.annotation.Scheduled;

public class RoutingTableMonitor {
	private final Logger logger = LoggerFactory.getLogger(getClass());

	private final RoutingTable routingTable;

	public RoutingTableMonitor(RoutingTable routingTable) {
		this.routingTable = routingTable;
	}

	// TODO: Configurable
	@Scheduled(fixedRateString = "5000", initialDelayString = "5000")
	public void printConnections() {
		logger.info("== Routing Table ==");
		routingTable.find(Tags.empty()).forEach(entry -> logger.info(entry.toString()));
	}
}
