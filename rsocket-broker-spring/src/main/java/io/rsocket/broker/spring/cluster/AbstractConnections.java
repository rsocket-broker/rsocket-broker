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

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.rsocket.RSocket;
import io.rsocket.broker.common.Id;
import io.rsocket.broker.frames.BrokerInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

/**
 * Maintains map of BrokerInfo to T of existing broker connections to current broker.
 * A T object should be able to resolve an RSocket.
 */
public abstract class AbstractConnections<T> {
	protected final Logger logger = LoggerFactory.getLogger(getClass());

	protected final Sinks.Many<BrokerInfoEntry<T>> joinEvents = Sinks.many().multicast().directBestEffort();
	protected final Sinks.Many<BrokerInfo> leaveEvents = Sinks.many().multicast().directBestEffort();

	protected final Map<Id, BrokerInfoEntry<T>> connections = new ConcurrentHashMap<>();

	public boolean contains(BrokerInfo brokerInfo) {
		return connections.containsKey(brokerInfo.getBrokerId());
	}

	public Collection<BrokerInfoEntry<T>> entries() {
		return connections.values();
	}

	public T get(BrokerInfo brokerInfo) {
		BrokerInfoEntry<T> entry = connections.get(brokerInfo.getBrokerId());
		if (entry == null) {
			return null;
		}
		return entry.value;
	}

	public T put(BrokerInfo brokerInfo, T connection) {
		logger.debug("adding {} RSocket {}", brokerInfo, connection);
		BrokerInfoEntry<T> entry = new BrokerInfoEntry<>(connection, brokerInfo);
		BrokerInfoEntry<T> old = connections.put(brokerInfo.getBrokerId(), entry);
		if (old == null) { // new connection
			joinEvents.tryEmitNext(entry);
			registerCleanup(brokerInfo, connection);
			return null;
		}
		return old.value;
	}

	public T remove(BrokerInfo brokerInfo) {
		BrokerInfoEntry<T> removed = connections.remove(brokerInfo.getBrokerId());
		if (removed != null) {
			leaveEvents.tryEmitNext(removed.getBrokerInfo());
			return removed.value;
		}
		return null;
	}

	protected abstract Mono<RSocket> getRSocket(T connection);

	protected void registerCleanup(BrokerInfo brokerInfo, T connection) {
		getRSocket(connection).flatMap(rSocket -> rSocket.onClose().doFinally(signal -> {
			// cleanup everything related to this connection
			logger.debug("removing connection {}", brokerInfo);
			connections.remove(brokerInfo.getBrokerId());
			leaveEvents.tryEmitNext(brokerInfo);

			// TODO: remove routes for broker
		})).subscribe();
	}

	public Flux<BrokerInfoEntry<T>> joinEvents() {
		return Flux.mergeSequential(Flux.fromIterable(connections.values()),
				joinEvents.asFlux()
		);
	}

	public Flux<BrokerInfo> leaveEvents() {
		return leaveEvents.asFlux();
	}

	public static class BrokerInfoEntry<T> {
		final T value;
		final BrokerInfo brokerInfo;

		public BrokerInfoEntry(T value, BrokerInfo brokerInfo) {
			this.value = value;
			this.brokerInfo = brokerInfo;
		}

		public T getValue() {
			return this.value;
		}

		public BrokerInfo getBrokerInfo() {
			return this.brokerInfo;
		}
	}
}
