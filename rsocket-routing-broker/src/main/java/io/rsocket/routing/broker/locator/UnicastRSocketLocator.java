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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.rsocket.RSocket;
import io.rsocket.loadbalance.LoadbalanceStrategy;
import io.rsocket.loadbalance.ResolvingRSocket;
import io.rsocket.routing.broker.RoutingTable;
import io.rsocket.routing.broker.query.RSocketQuery;
import io.rsocket.routing.common.Tags;
import io.rsocket.routing.common.WellKnownKey;
import io.rsocket.routing.frames.Address;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RSocketLocator that finds exactly one RSocket for a particular Address. If more than one RSocket is found by
 * RSocketQuery, then load balancing is used to select one.
 */
public class UnicastRSocketLocator implements RSocketLocator {
	private static final Logger logger = LoggerFactory.getLogger(UnicastRSocketLocator.class);

	private final RSocketQuery rSocketQuery;
	private final RoutingTable routingTable;
	private final String defaultLoadBalancer;
	private final Map<String, LoadbalanceStrategy> loadbalancers = new HashMap<>();

	public UnicastRSocketLocator(RSocketQuery rSocketQuery, RoutingTable routingTable, List<Loadbalancer> loadbalancers, String defaultLoadBalancer) {
		this.rSocketQuery = rSocketQuery;
		this.routingTable = routingTable;
		this.defaultLoadBalancer = defaultLoadBalancer;
		for (Loadbalancer loadbalancer : loadbalancers) {
			if (this.loadbalancers.containsKey(loadbalancer.name()) && logger.isWarnEnabled()) {
				logger.warn(loadbalancer.name() + " Loadbalancer already exists, overwriting.");
			}
			this.loadbalancers.put(loadbalancer.name(), loadbalancer.strategy());
		}
		if (!this.loadbalancers.containsKey(defaultLoadBalancer)) {
			throw new IllegalStateException("No Loadbalancer for " + defaultLoadBalancer + ". Found " + this.loadbalancers.keySet());
		}
	}

	@Override
	public boolean supports(Address.RoutingType routingType) {
		return routingType == Address.RoutingType.UNICAST;
	}

	@Override
	public RSocket locate(Address address) {
		List<RSocket> found = rSocketQuery.query(address.getTags());
		final int size = found.size();
		switch (size) {
		case 0:
			return resolvingRSocket(address.getTags());
		case 1:
			return found.get(0);
		default:
			return loadbalance(found, address.getTags());
		}
	}

	private ResolvingRSocket resolvingRSocket(Tags tags) {
		return new ResolvingRSocket(routingTable.joinEvents(tags)
				.next()
				.map(routeSetup -> {
					List<RSocket> found = rSocketQuery.query(tags);
					if (logger.isWarnEnabled() && found.isEmpty()) {
						logger.warn("Unable to locate RSockets for tags {}", tags);
					}
					return loadbalance(found, tags);
				}));
	}

	private RSocket loadbalance(List<RSocket> rSockets, Tags tags) {
		LoadbalanceStrategy strategy = null;
		if (tags.containsKey(WellKnownKey.LB_METHOD)) {
			String lbMethod = tags.get(WellKnownKey.LB_METHOD);
			if (loadbalancers.containsKey(lbMethod)) {
				strategy = loadbalancers.get(lbMethod);
			}
		}
		if (strategy == null) {
			strategy = loadbalancers.get(defaultLoadBalancer);
		}
		return strategy.select(rSockets);
	}

	public interface Loadbalancer {
		String name();

		LoadbalanceStrategy strategy();
	}
}
