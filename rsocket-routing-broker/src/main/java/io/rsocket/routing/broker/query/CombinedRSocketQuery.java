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

package io.rsocket.routing.broker.query;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import io.netty.util.concurrent.FastThreadLocal;
import io.rsocket.RSocket;
import io.rsocket.routing.broker.RSocketIndex;
import io.rsocket.routing.broker.RoutingTable;
import io.rsocket.routing.common.Id;
import io.rsocket.routing.common.Tags;
import io.rsocket.routing.frames.BrokerInfo;
import io.rsocket.routing.frames.RouteJoin;

/**
 * RSocketQuery that merges RSockets from the local index (RSocketIndex) and from remote brokers (RoutingTable).
 */
public class CombinedRSocketQuery implements RSocketQuery {

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

	private final Id brokerId;
	private final RoutingTable routingTable;
	private final RSocketIndex rSocketIndex;
	private final Function<BrokerInfo, RSocket> brokerInfoRSocketMapper;

	public CombinedRSocketQuery(Id brokerId, RoutingTable routingTable,
			RSocketIndex rSocketIndex,
			Function<BrokerInfo, RSocket> brokerInfoRSocketMapper) {
		this.brokerId = brokerId;
		this.routingTable = routingTable;
		this.rSocketIndex = rSocketIndex;
		this.brokerInfoRSocketMapper = brokerInfoRSocketMapper;
	}

	@Override
	public List<RSocket> query(Tags tags) {
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
				members.add(brokerInfoRSocketMapper.apply(brokerInfo));
			}
		}

		return members;
	}

}
