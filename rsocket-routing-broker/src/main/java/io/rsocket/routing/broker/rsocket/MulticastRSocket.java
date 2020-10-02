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

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * RSocket implementation that will broadcast payloads to a collection of RSockets.
 * If the interaction model is requestResponse this should only emit one item.
 * In this case it will emit the first item that is returned.
 */
public class MulticastRSocket implements RSocket {

	private final Supplier<List<? extends RSocket>> rSocketListSupplier;

	public MulticastRSocket(Supplier<List<? extends RSocket>> rSocketListSupplier) {
		this.rSocketListSupplier = rSocketListSupplier;
	}

	public Collection<? extends RSocket> getRSockets() {
		return rSocketListSupplier.get();
	}

	@Override
	public Mono<Void> fireAndForget(Payload payload) {
		Collection<? extends RSocket> rSockets = getRSockets();

		if (rSockets.isEmpty()) {
			payload.release();
			return Mono.empty();
		}
		else if (rSockets.size() > 1) {
			payload.retain(rSockets.size() - 1);
		}

		return Flux.fromIterable(rSockets)
				.flatMap(rSocket -> rSocket.fireAndForget(payload))
				.ignoreElements();
	}

	@Override
	public Mono<Payload> requestResponse(Payload payload) {
		Collection<? extends RSocket> rSockets = getRSockets();

		if (rSockets.isEmpty()) {
			payload.release();
			return Mono.empty();
		}
		else if (rSockets.size() > 1) {
			payload.retain(rSockets.size() - 1);
		}

		return Mono.create(
				sink -> {
					Composite composite = Disposables.composite();
					sink.onDispose(composite);
					for (RSocket rSocket : rSockets) {
						Disposable disposable =
								rSocket
										.requestResponse(payload)
										.doOnCancel(() -> {
										})
										.subscribe(sink::success, sink::error, sink::success);
						composite.add(disposable);
					}
				});
	}

	@Override
	public Flux<Payload> requestStream(Payload payload) {
		Collection<? extends RSocket> rSockets = getRSockets();

		if (rSockets.isEmpty()) {
			payload.release();
			return Flux.empty();
		}
		else if (rSockets.size() > 1) {
			payload.retain(rSockets.size() - 1);
		}

		return Flux.fromIterable(rSockets).flatMap(rSocket -> rSocket.requestStream(payload));
	}

	@Override
	public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
		Collection<? extends RSocket> rSockets = getRSockets();

		if (rSockets.isEmpty()) {
			return Flux.empty();
		}
		else if (rSockets.size() > 1) {
			payloads = Flux.from(payloads).map(payload -> payload.retain(rSockets.size() - 1));
		}

		Publisher<Payload> _p = payloads;
		return Flux.fromIterable(rSockets).flatMap(rSocket -> rSocket.requestChannel(_p));
	}

	@Override
	public Mono<Void> metadataPush(Payload payload) {
		Collection<? extends RSocket> rSockets = getRSockets();

		if (rSockets.size() > 1) {
			payload.retain(rSockets.size() - 1);
		}

		return Flux.fromIterable(rSockets)
				.flatMap(rSocket -> rSocket.metadataPush(payload))
				.ignoreElements();
	}
}
