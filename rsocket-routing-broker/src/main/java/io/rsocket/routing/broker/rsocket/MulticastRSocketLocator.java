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

package io.rsocket.routing.broker.rsocket;

import io.rsocket.RSocket;
import io.rsocket.routing.broker.locator.RSocketLocator;
import io.rsocket.routing.broker.query.RSocketQuery;
import io.rsocket.routing.frames.Address;
import io.rsocket.routing.frames.RoutingType;

/**
 * RSocketLocator that returns a MulticastRSocket that uses all matching RSocket instances and combines
 * requests and responses according to the the Routing and Forwarding specification.
 */
public class MulticastRSocketLocator implements RSocketLocator {

	private final RSocketQuery rSocketQuery;

	public MulticastRSocketLocator(RSocketQuery rSocketQuery) {
		this.rSocketQuery = rSocketQuery;
	}

	@Override
	public boolean supports(RoutingType routingType) {
		return routingType == RoutingType.MULTICAST;
	}

	@Override
	public RSocket locate(Address address) {
		return new MulticastRSocket(() -> rSocketQuery.query(address.getTags()));
	}
}
