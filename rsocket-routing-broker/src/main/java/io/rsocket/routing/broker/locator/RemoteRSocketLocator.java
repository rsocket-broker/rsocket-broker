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
import java.util.List;

import io.netty.util.concurrent.FastThreadLocal;
import io.rsocket.AbstractRSocket;
import io.rsocket.RSocket;
import io.rsocket.routing.broker.RSocketIndex;
import io.rsocket.routing.broker.RoutingTable;
import io.rsocket.routing.broker.rsocket.ConnectingRSocket;
import io.rsocket.routing.common.Tags;

public class RemoteRSocketLocator implements RSocketLocator {

	private static final FastThreadLocal<List<RSocket>> MEMBERS;

	static {
		MEMBERS = new FastThreadLocal<List<RSocket>>() {
			@Override
			protected List<RSocket> initialValue() {
				return new ArrayList<>();
			}
		};

	}

	private final RoutingTable routingTable;
	private final RSocketIndex rSocketIndex;

	public RemoteRSocketLocator(RoutingTable routingTable, RSocketIndex rSocketIndex) {
		this.routingTable = routingTable;
		this.rSocketIndex = rSocketIndex;
	}

	private List<RSocket> members(Tags tags) {
		List<RSocket> members = MEMBERS.get();
		members.clear();
		List<RSocket> query = rSocketIndex.query(tags);
		if (query != null && !query.isEmpty()) {
			members.addAll(query);
		}

		// TODO: remote? or should rSocketIndex have all?

		return members;
	}

	@Override
	public RSocket apply(Tags tags) {
		// TODO: broadcast

		List<RSocket> members = members(tags);
		if (members.isEmpty()) {
			return new ConnectingRSocket(routingTable.joinEvents(tags).next()
					.map(routeSetup -> loadbalance(members(tags))));
		}

		return loadbalance(members);
	}

	private RSocket loadbalance(List<RSocket> rSockets) {
		if (rSockets == null || rSockets.isEmpty()) {
			// return emtpty?
			return new AbstractRSocket() {
			};
		}
		// TODO: loadbalance
		return rSockets.get(0);
	}
}
