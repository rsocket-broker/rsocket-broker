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

import java.util.function.Function;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.routing.broker.locator.RSocketLocator;
import io.rsocket.routing.common.Tags;

/**
 * Reusable factor for RoutingRSocket.
 */
public class RoutingRSocketFactory {

	private final RSocketLocator rSocketLocator;
	private final Function<Payload, Tags> tagsExtractor;
	private final Function<RSocket, RSocket> rSocketTransformer;

	public RoutingRSocketFactory(RSocketLocator rSocketLocator, Function<Payload, Tags> tagsExtractor,
			Function<RSocket, RSocket> rSocketTransformer) {
		this.rSocketLocator = rSocketLocator;
		this.tagsExtractor = tagsExtractor;
		this.rSocketTransformer = rSocketTransformer;
	}

	public RSocket create() {
		return rSocketTransformer.apply(new RoutingRSocket(rSocketLocator, tagsExtractor));
	}
}
