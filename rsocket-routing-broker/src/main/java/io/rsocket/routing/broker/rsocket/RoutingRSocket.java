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
import io.rsocket.routing.frames.Address;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Routes received requests to the correct routable destination.
 */
public class RoutingRSocket implements RSocket {

	private final RSocketLocator rSocketLocator;
	private final Function<Payload, Address> addressExtractor;

	public RoutingRSocket(RSocketLocator rSocketLocator, Function<Payload, Address> addressExtractor) {
		this.rSocketLocator = rSocketLocator;
		this.addressExtractor = addressExtractor;
	}

	@Override
	public Mono<Void> fireAndForget(Payload payload) {
		try {
			RSocket located = locate(payload);

			return located.fireAndForget(payload);
		} catch (Throwable t) {
			payload.release();
			return Mono.error(handleError(t));
		}
	}

	@Override
	public Mono<Payload> requestResponse(Payload payload) {
		try {
			RSocket located = locate(payload);

			return located.requestResponse(payload);
		} catch (Throwable t) {
			payload.release();
			return Mono.error(handleError(t));
		}
	}

	@Override
	public Mono<Void> metadataPush(Payload payload) {
		try {
			RSocket located = locate(payload);

			return located.metadataPush(payload);
		} catch (Throwable t) {
			payload.release();
			return Mono.error(handleError(t));
		}
	}

	@Override
	public Flux<Payload> requestStream(Payload payload) {
		try {
			RSocket located = locate(payload);

			return located.requestStream(payload);
		} catch (Throwable t) {
			payload.release();
			return Flux.error(handleError(t));
		}
	}

	@Override
	public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
		return Flux.from(payloads).switchOnFirst((first, flux) -> {
			if (first.hasValue()) {
				Payload payload = first.get();
				try {
					RSocket located = locate(payload);
					return located.requestChannel(flux);
				}
				catch (Throwable t) {
					//noinspection ConstantConditions
					payload.release();
					return Flux.error(handleError(t));
				}
			}
			return flux;
		});
	}

	private RSocket locate(Payload payload) {
		Address address = addressExtractor.apply(payload);
		if (!rSocketLocator.supports(address.getRoutingType())) {
			throw new IllegalStateException("No RSocketLocator for RoutingType " + address.getRoutingType());
		}
		return rSocketLocator.locate(address);
	}

	private Throwable handleError(Throwable t) {
		//TODO: how to do error handling here?
		return t;
	}

}
