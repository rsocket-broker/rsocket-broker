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

package io.rsocket.routing.broker.locator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import io.netty.util.concurrent.FastThreadLocal;
import io.rsocket.AbstractRSocket;
import io.rsocket.RSocket;
import io.rsocket.routing.broker.RSocketIndex;
import io.rsocket.routing.broker.RoutingTable;
import io.rsocket.routing.broker.config.BrokerProperties;
import io.rsocket.routing.broker.rsocket.ConnectingRSocket;
import io.rsocket.routing.common.Id;
import io.rsocket.routing.common.Tags;
import io.rsocket.routing.frames.BrokerInfo;
import io.rsocket.routing.frames.RouteJoin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RSocketLocator that merges RSockets from the local index and from remote brokers.
 */
public class RemoteRSocketLocator implements RSocketLocator {

	private static final Logger logger = LoggerFactory
			.getLogger(RemoteRSocketLocator.class);

	private static final FastThreadLocal<List<RSocket>> MEMBERS;
	private static final FastThreadLocal<Set<Id>> FOUND;

	static {
		MEMBERS = new FastThreadLocal<List<RSocket>>() {
			@Override
			protected List<RSocket> initialValue() {
				return new ArrayList<>();
			}
		};

		FOUND = new FastThreadLocal<Set<Id>>() {
			@Override
			protected Set<Id> initialValue() {
				return new HashSet<>();
			}
		};
	}

	private final BrokerProperties properties;
	private final RoutingTable routingTable;
	private final RSocketIndex rSocketIndex;
	private final Function<BrokerInfo, RSocket> brokerInfoRSocketFunction;

	public RemoteRSocketLocator(BrokerProperties properties, RoutingTable routingTable,
			RSocketIndex rSocketIndex, Function<BrokerInfo, RSocket> brokerInfoRSocketFunction) {
		this.properties = properties;
		this.routingTable = routingTable;
		this.rSocketIndex = rSocketIndex;
		this.brokerInfoRSocketFunction = brokerInfoRSocketFunction;
	}

	private List<RSocket> members(Tags tags) {
		List<RSocket> members = MEMBERS.get();
		members.clear();
		List<RSocket> query = rSocketIndex.query(tags);
		if (query != null && !query.isEmpty()) {
			members.addAll(query);
		}

		// find remote brokers
		Set<Id> found = FOUND.get();
		found.clear();

		for (RouteJoin routeJoin : routingTable.find(tags)) {
			Id brokerId = routeJoin.getBrokerId();
			if (!Objects.equals(properties.getBrokerId(), brokerId) && !found
					.contains(brokerId)) {
				found.add(brokerId);

				BrokerInfo brokerInfo = BrokerInfo.from(brokerId).build();
				members.add(brokerInfoRSocketFunction.apply(brokerInfo));
			}
		}

		return members;
	}

	@Override
	public RSocket apply(Tags tags) {
		// TODO: broadcast

		List<RSocket> members = members(tags);
		if (members.isEmpty()) {
			return new ConnectingRSocket(routingTable.joinEvents(tags)
					.next()
					.map(routeSetup -> {
						List<RSocket> found = members(tags);
						if (logger.isWarnEnabled() && (found == null || found
								.isEmpty())) {
							logger.warn("Unable to locate RSockets for tags {}", tags);
						}
						return loadbalance(found);
					}));
		}

		return loadbalance(members);
	}

	private RSocket loadbalance(List<RSocket> rSockets) {
		if (rSockets == null || rSockets.isEmpty()) {
			// TODO: return empty?
			return new AbstractRSocket() {
			};
		}
		// TODO: loadbalance
		return rSockets.get(0);
	}
}
