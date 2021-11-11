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

package io.rsocket.routing.broker;

import java.util.List;
import java.util.function.Predicate;

import io.rsocket.routing.broker.util.IndexedMap;
import io.rsocket.routing.broker.util.RoaringBitmapIndexedMap;
import io.rsocket.routing.common.Id;
import io.rsocket.routing.common.Tags;
import io.rsocket.routing.frames.RouteJoin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

/**
 * Maintains index of RouteJoin objects. Actions include find, add and remove. Also
 * streams events with a RouteJoin is added or removed.
 */
// TODO: use some other object rather than RouteJoin
public class RoutingTable implements Disposable {

	private static final Logger logger = LoggerFactory.getLogger(RoutingTable.class);

	private final IndexedMap<Id, RouteJoin, Tags> routes = new RoaringBitmapIndexedMap<>();
	private final Sinks.Many<RouteJoin> joinEvents = Sinks.many().multicast().directBestEffort();
	private final Sinks.Many<RouteJoin> leaveEvents = Sinks.many().multicast().directBestEffort();

	public RouteJoin find(Id routeId) {
		synchronized (routes) {
			return routes.get(routeId);
		}
	}

	public List<RouteJoin> find(Tags tags) {
		synchronized (routes) {
			return routes.query(tags);
		}
	}

	public void add(RouteJoin routeJoin) {
		logger.debug("adding RouteJoin {}", routeJoin);
		synchronized (routes) {
			// TODO: check timestamp
			routes.put(routeJoin.getRouteId(), routeJoin, routeJoin.getTags());
		}
		joinEvents.tryEmitNext(routeJoin);
	}

	// TODO: remove with timestamp
	public void remove(Id routeId) {
		logger.debug("removing routeId {}", routeId);
		synchronized (routes) {
			RouteJoin routeJoin = routes.remove(routeId);
			if (routeJoin != null) {
				leaveEvents.tryEmitNext(routeJoin);
			}
		}
	}

	public Flux<RouteJoin> joinEvents(Predicate<RouteJoin> predicate) {
		// merge with existing routes
		return joinEvents.asFlux().filter(predicate);
	}

	public Flux<RouteJoin> joinEvents(Tags tags) {
		return joinEvents(containsTags(tags));
	}

	public Flux<RouteJoin> leaveEvents(Tags tags) {
		return leaveEvents.asFlux().filter(containsTags(tags));
	}

	@Override
	public void dispose() {
		routes.clear();
	}

	static Predicate<RouteJoin> containsTags(Tags tags) {
		return event -> {
			boolean contains = event.getTags().entries().containsAll(tags.entries());
			return contains;
		};
	}
}
