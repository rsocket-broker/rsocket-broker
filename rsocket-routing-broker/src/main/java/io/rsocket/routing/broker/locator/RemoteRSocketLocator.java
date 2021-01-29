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
import io.rsocket.RSocket;
import io.rsocket.loadbalance.LoadbalanceStrategy;
import io.rsocket.routing.broker.RSocketIndex;
import io.rsocket.routing.broker.RoutingTable;
import io.rsocket.routing.broker.rsocket.MulticastRSocket;
import io.rsocket.routing.broker.rsocket.ResolvingRSocket;
import io.rsocket.routing.common.Id;
import io.rsocket.routing.common.Tags;
import io.rsocket.routing.frames.Address;
import io.rsocket.routing.frames.BrokerInfo;
import io.rsocket.routing.frames.RouteJoin;
import io.rsocket.routing.frames.RoutingType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RSocketLocator that merges RSockets from the local index and from remote brokers.
 */
public class RemoteRSocketLocator implements RSocketLocator {

	private static final Logger logger = LoggerFactory.getLogger(RemoteRSocketLocator.class);

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

	private final Id                            brokerId;
	private final RoutingTable                  routingTable;
	private final RSocketIndex                  rSocketIndex;
	private final LoadbalanceStrategy           loadbalanceStrategy;
	private final Function<BrokerInfo, RSocket> brokerInfoRSocketFunction;

	public RemoteRSocketLocator(Id brokerId, RoutingTable routingTable,
			RSocketIndex rSocketIndex, LoadbalanceStrategy loadbalanceStrategy,
			Function<BrokerInfo, RSocket> brokerInfoRSocketFunction) {
		this.brokerId = brokerId;
		this.routingTable = routingTable;
		this.rSocketIndex = rSocketIndex;
		this.loadbalanceStrategy = loadbalanceStrategy;
		this.brokerInfoRSocketFunction = brokerInfoRSocketFunction;
	}

	private List<RSocket> members(Tags tags) {
		// TODO: should this be configurable?
		if (tags == null || tags.isEmpty()) {
			// fail if tags are emtpy
			throw new IllegalArgumentException("tags may not be empty");
		}
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
			Id joinedBrokerId = routeJoin.getBrokerId();
			if (!Objects.equals(this.brokerId, joinedBrokerId) && !found
					.contains(joinedBrokerId)) {
				found.add(joinedBrokerId);

				BrokerInfo brokerInfo = BrokerInfo.from(joinedBrokerId).build();
				members.add(brokerInfoRSocketFunction.apply(brokerInfo));
			}
		}

		return members;
	}

	@Override
	public RSocket apply(Address address) {
		Tags tags = address.getTags();

		// multicast
		if (address.getRoutingType() == RoutingType.MULTICAST) {
			return new MulticastRSocket(() -> members(tags));
		}

		// unicast
		List<RSocket> members = members(tags);
		final int size = members.size();
		switch (size) {
			case 0:
				return connectingRSocket(tags);
			case 1:
				return members.get(0);
			default:
				return loadbalance(members, tags);
		}
	}

	private ResolvingRSocket connectingRSocket(Tags tags) {
		return new ResolvingRSocket(routingTable.joinEvents(tags)
				.next()
				.map(routeSetup -> {
					List<RSocket> found = members(tags);
					if (logger.isWarnEnabled() && found.isEmpty()) {
						logger.warn("Unable to locate RSockets for tags {}", tags);
					}
					return loadbalance(found, tags);
				}));
	}

	private RSocket loadbalance(List<RSocket> rSockets, Tags tags) {
		return loadbalanceStrategy.select(rSockets);
	}
}
