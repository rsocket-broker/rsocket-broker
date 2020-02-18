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

package io.rsocket.routing.broker.loadbalance;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import io.rsocket.RSocket;
import io.rsocket.routing.common.Tags;
import reactor.core.publisher.Mono;

public class RoundRobinLoadBalancer implements LoadBalancer {

	private final AtomicInteger position;

	public RoundRobinLoadBalancer() {
		this(new Random().nextInt(1000));
	}

	public RoundRobinLoadBalancer(int seedPosition) {
		this.position = new AtomicInteger(seedPosition);
	}

	@Override
	public Mono<Response> choose(Request request) {

		List<RSocket> rSockets = request.getRSockets();
		if (rSockets == null || rSockets.isEmpty()) {
			return Mono.just(new Response(null));
		}

		// TODO: enforce order?
		int pos = Math.abs(this.position.incrementAndGet());
		RSocket rSocket = rSockets.get(pos % rSockets.size());

		return Mono.just(new Response(rSocket));
	}

	public static class Factory implements LoadBalancer.Factory {

		private final Map<Tags, RoundRobinLoadBalancer> loadBalancers = new ConcurrentHashMap<>();

		//TODO: listen for remove events.

		@Override
		public LoadBalancer getInstance(Tags tags) {
			return loadBalancers.computeIfAbsent(tags, t -> new RoundRobinLoadBalancer());
		}
	}
}
