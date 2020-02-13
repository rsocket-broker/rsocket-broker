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

package io.rsocket.routing.broker.spring.cluster;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.rsocket.routing.frames.BrokerInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;

import org.springframework.messaging.rsocket.RSocketRequester;

/**
 * Maintains map of BrokerInfo->RSocketRequester of existing broker connections to current broker.
 */
public class BrokerConnections {
	private final Logger logger = LoggerFactory.getLogger(getClass());

	private final FluxProcessor<BrokerInfo, BrokerInfo> joinEvents =
			DirectProcessor.<BrokerInfo>create().serialize();
	private final FluxProcessor<BrokerInfo, BrokerInfo> leaveEvents =
			DirectProcessor.<BrokerInfo>create().serialize();

	// TODO: add broker add and leave streams like RoutingTable
	private final Map<BrokerInfo, RSocketRequester> brokerConnections = new ConcurrentHashMap<>();

	public boolean contains(BrokerInfo brokerInfo) {
		return brokerConnections.containsKey(brokerInfo);
	}

	public RSocketRequester get(BrokerInfo brokerInfo) {
		return brokerConnections.get(brokerInfo);
	}

	public RSocketRequester put(BrokerInfo brokerInfo, RSocketRequester requester) {
		logger.debug("adding {} RSocket {}", brokerInfo, requester);
		RSocketRequester old = brokerConnections.put(brokerInfo, requester);
		joinEvents.onNext(brokerInfo);
		registerCleanup(brokerInfo, requester);
		return old;
	}

	public RSocketRequester remove(BrokerInfo brokerInfo) {
		RSocketRequester removed = brokerConnections.remove(brokerInfo);
		leaveEvents.onNext(brokerInfo);
		return removed;
	}

	private void registerCleanup(BrokerInfo brokerInfo, RSocketRequester rSocketRequester) {
		rSocketRequester.rsocket().onClose().doFinally(signal -> {
			// cleanup everything related to this connection
			logger.info("removing connection " + brokerInfo);
			brokerConnections.remove(brokerInfo);
			leaveEvents.onNext(brokerInfo);

			// TODO: remove routes for broker
		});
	}

	public Flux<BrokerInfo> joinEvents() {
		return joinEvents.filter(brokerInfo -> true);
	}

	public Flux<BrokerInfo> leaveEvents() {
		return leaveEvents.filter(brokerInfo -> true);
	}

}
