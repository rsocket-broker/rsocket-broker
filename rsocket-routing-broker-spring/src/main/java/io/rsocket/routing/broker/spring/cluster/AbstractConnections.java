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
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import io.rsocket.RSocket;
import io.rsocket.routing.frames.BrokerInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;

/**
 * Maintains map of BrokerInfo to T of existing broker connections to current broker.
 * A T object should be able to resolve an RSocket.
 */
public abstract class AbstractConnections<T> {
	protected final Logger logger = LoggerFactory.getLogger(getClass());

	protected final FluxProcessor<BrokerInfo, BrokerInfo> joinEvents =
			DirectProcessor.<BrokerInfo>create().serialize();
	protected final FluxProcessor<BrokerInfo, BrokerInfo> leaveEvents =
			DirectProcessor.<BrokerInfo>create().serialize();

	protected final Map<BrokerInfo, T> connections = new ConcurrentHashMap<>();

	public boolean contains(BrokerInfo brokerInfo) {
		return connections.containsKey(brokerInfo);
	}

	public Set<Entry<BrokerInfo, T>> entries() {
		return connections.entrySet();
	}

	public T get(BrokerInfo brokerInfo) {
		return connections.get(brokerInfo);
	}

	public T put(BrokerInfo brokerInfo, T connection) {
		logger.debug("adding {} RSocket {}", brokerInfo, connection);
		T old = connections.put(brokerInfo, connection);
		joinEvents.onNext(brokerInfo);
		registerCleanup(brokerInfo, connection);
		return old;
	}

	public T remove(BrokerInfo brokerInfo) {
		T removed = connections.remove(brokerInfo);
		leaveEvents.onNext(brokerInfo);
		return removed;
	}

	protected abstract RSocket getRSocket(T connection);

	protected void registerCleanup(BrokerInfo brokerInfo, T connection) {
		getRSocket(connection).onClose().doFinally(signal -> {
			// cleanup everything related to this connection
			logger.debug("removing connection {}", brokerInfo);
			connections.remove(brokerInfo);
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
