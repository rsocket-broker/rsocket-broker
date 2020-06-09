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
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Routes received requests to the correct routable destination.
 */
public class RoutingRSocket implements RSocket {

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
			Mono<RSocket> located = rSocketLocator.apply(tags);

			//TODO: metadata enrichment?

			return located.map(rSocket -> rSocket.fireAndForget(payload)
					.onErrorResume(e -> Mono.error(new RuntimeException("TODO fnf", e)))).then();
		} catch (Throwable e) {
			payload.release();
			return Mono.error(new RuntimeException("TODO: fill out values", e)); //TODO:
		}
	}

	@Override
	public Mono<Payload> requestResponse(Payload payload) {
		try {
			Tags tags = tagsExtractor.apply(payload);
			Mono<RSocket> located = rSocketLocator.apply(tags);

			return located.flatMap(rSocket -> rSocket.requestResponse(payload)
					.onErrorResume(e -> Mono.error(new RuntimeException("TODO rr", e))));
		} catch (Throwable e) {
			payload.release();
			return Mono.error(new RuntimeException("TODO: fill out values", e)); //TODO:
		}
	}

	@Override
	public Mono<Void> metadataPush(Payload payload) {
		try {
			Tags tags = tagsExtractor.apply(payload);
			Mono<RSocket> located = rSocketLocator.apply(tags);

			return located.map(rSocket -> rSocket.metadataPush(payload)
					.onErrorResume(e -> Mono.error(new RuntimeException("TODO mp", e)))).then();
		} catch (Throwable e) {
			payload.release();
			return Mono.error(new RuntimeException("TODO: fill out values", e)); //TODO:
		}
	}

	@Override
	public Flux<Payload> requestStream(Payload payload) {
		try {
			Tags tags = tagsExtractor.apply(payload);
			Mono<RSocket> located = rSocketLocator.apply(tags);

			return located.flatMapMany(rSocket -> rSocket.requestStream(payload)
					.onErrorResume(e -> Flux.error(new RuntimeException("TODO", e))));
		} catch (Throwable e) {
			payload.release();
			return Flux.error(new RuntimeException("TODO: fill out values", e)); //TODO:
		}
	}

	@Override
	public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
		return Flux.from(payloads).switchOnFirst((first, flux) -> {
			if (first.hasValue()) {
				Payload payload = first.get();
				payload.retain();
				try {
					Tags tags = tagsExtractor.apply(payload);
					Mono<RSocket> located = rSocketLocator.apply(tags);
					return located.flatMapMany(rSocket -> rSocket.requestChannel(flux.skip(1).startWith(payload))
							.onErrorResume(e -> Flux.error(new RuntimeException("TODO rc", e))));
				}
				catch (Throwable e) {
					payload.release();
					return Flux
							.error(new RuntimeException("TODO: fill out values", e)); //TODO:
				}
			}
			return flux;
		});
	}

}
