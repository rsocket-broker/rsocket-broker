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

import io.rsocket.AbstractRSocket;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.ResponderRSocket;
import io.rsocket.routing.broker.locator.RSocketLocator;
import io.rsocket.routing.common.Tags;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Routes received requests to the correct routable destination.
 */
public class RoutingRSocket extends AbstractRSocket implements ResponderRSocket {

	private final RSocketLocator rSocketLocator;
	private final Function<Payload, Tags> tagsExtractor;

	public RoutingRSocket(RSocketLocator rSocketLocator, Function<Payload, Tags> tagsExtractor) {
		this.rSocketLocator = rSocketLocator;
		this.tagsExtractor = tagsExtractor;
	}

	@Override
	public Mono<Void> fireAndForget(Payload payload) {
		try {
			Tags tags = tagsExtractor.apply(payload);
			RSocket rSocket = rSocketLocator.apply(tags);

			//TODO: metadata enrichment?

			return rSocket.fireAndForget(payload)
					.onErrorResume(e -> Mono.error(new RuntimeException("TODO")));
		} catch (Throwable e) {
			payload.release();
			return Mono.error(new RuntimeException("TODO: fill out values")); //TODO:
		}
	}

	@Override
	public Mono<Payload> requestResponse(Payload payload) {
		try {
			Tags tags = tagsExtractor.apply(payload);
			RSocket rSocket = rSocketLocator.apply(tags);

			return rSocket.requestResponse(payload)
					.onErrorResume(e -> Mono.error(new RuntimeException("TODO")));
		} catch (Throwable e) {
			payload.release();
			return Mono.error(new RuntimeException("TODO: fill out values")); //TODO:
		}
	}

	@Override
	public Mono<Void> metadataPush(Payload payload) {
		try {
			Tags tags = tagsExtractor.apply(payload);
			RSocket rSocket = rSocketLocator.apply(tags);

			return rSocket.metadataPush(payload)
					.onErrorResume(e -> Mono.error(new RuntimeException("TODO")));
		} catch (Throwable e) {
			payload.release();
			return Mono.error(new RuntimeException("TODO: fill out values")); //TODO:
		}
	}

	@Override
	public Flux<Payload> requestStream(Payload payload) {
		try {
			Tags tags = tagsExtractor.apply(payload);
			RSocket rSocket = rSocketLocator.apply(tags);

			return rSocket.requestStream(payload)
					.onErrorResume(e -> Flux.error(new RuntimeException("TODO")));
		} catch (Throwable e) {
			payload.release();
			return Flux.error(new RuntimeException("TODO: fill out values")); //TODO:
		}
	}

	@Override
	public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
		return Flux.from(payloads).switchOnFirst((first, flux) -> {
			if (first.hasValue()) {
				Payload payload = first.get();
				try {
					Tags tags = tagsExtractor.apply(payload);
					RSocket rSocket = rSocketLocator.apply(tags);
					return rSocket.requestChannel(flux.skip(1).startWith(payload))
							.onErrorResume(e -> Flux.error(new RuntimeException("TODO")));
				}
				catch (Throwable e) {
					payload.release();
					return Flux
							.error(new RuntimeException("TODO: fill out values")); //TODO:
				}
			}
			return flux;
		});
	}

	@Override
	public Flux<Payload> requestChannel(Payload payload, Publisher<Payload> payloads) {
		try {
			Tags tags = tagsExtractor.apply(payload);
			RSocket rSocket = rSocketLocator.apply(tags);

			return rSocket.requestChannel(Flux.from(payloads).skip(1).startWith(payload))
					.onErrorResume(e -> Flux.error(new RuntimeException("TODO", e)));
		} catch (Throwable e) {
			payload.release();
			return Flux.error(new RuntimeException("TODO: fill out values")); //TODO:
		}
	}
}
