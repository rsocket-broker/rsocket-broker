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
import io.rsocket.routing.frames.RouteSetup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;

/**
 * Maintains index of RouteSetup objects. Actions include find, add and remove. Also
 * streams events with a RouteSetup is added or removed.
 */
public class RoutingTable implements Disposable {

	private static final Logger logger = LoggerFactory.getLogger(RoutingTable.class);

	private final IndexedMap<Id, RouteSetup, Tags> routes = new RoaringBitmapIndexedMap<>();
	private final FluxProcessor<RouteSetup, RouteSetup> joinEvents =
			DirectProcessor.<RouteSetup>create().serialize();
	private final FluxProcessor<RouteSetup, RouteSetup> leaveEvents =
			DirectProcessor.<RouteSetup>create().serialize();

	public RouteSetup find(Id routeId) {
		synchronized (routes) {
			return routes.get(routeId);
		}
	}

	public List<RouteSetup> find(Tags tags) {
		synchronized (routes) {
			return routes.query(tags);
		}
	}

	public void add(RouteSetup routeSetup) {
		logger.info("adding RouteSetup {}", routeSetup);
		synchronized (routes) {
			routes.put(routeSetup.getRouteId(), routeSetup, routeSetup.getTags());
		}
		joinEvents.onNext(routeSetup);
	}

	public void remove(Id routeId) {
		logger.info("removing routeId {}", routeId);
		synchronized (routes) {
			RouteSetup routeSetup = routes.remove(routeId);
			if (routeSetup != null) {
				leaveEvents.onNext(routeSetup);
			}
		}
	}

	public Flux<RouteSetup> joinEvents(Tags tags) {
		return joinEvents.filter(containsTags(tags));
	}

	public Flux<RouteSetup> leaveEvents(Tags tags) {
		return leaveEvents.filter(containsTags(tags));
	}

	@Override
	public void dispose() {
		routes.clear();
	}

	static Predicate<RouteSetup> containsTags(Tags tags) {
		return event -> event.getTags().entries().containsAll(tags.entries());
	}
}
