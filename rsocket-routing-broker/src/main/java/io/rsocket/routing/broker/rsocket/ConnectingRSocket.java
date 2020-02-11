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

import io.rsocket.Payload;
import io.rsocket.RSocket;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

/**
 * RSocket implementation used to wait for pending RSocket connections.
 */
public class ConnectingRSocket implements RSocket {

	private final MonoProcessor<RSocket> cachedRSocket;

	public ConnectingRSocket(Mono<RSocket> connectingRSocket) {
		this.cachedRSocket = connectingRSocket.toProcessor();
	}

	@Override
	public Mono<Void> fireAndForget(Payload payload) {
		if (isSuccess()) {
			return peek().fireAndForget(payload);
		}
		return cachedRSocket.flatMap(rSocket -> rSocket.fireAndForget(payload));
	}

	@Override
	public Mono<Payload> requestResponse(Payload payload) {
		if (isSuccess()) {
			return peek().requestResponse(payload);
		}
		return cachedRSocket.flatMap(rSocket -> rSocket.requestResponse(payload));
	}

	@Override
	public Flux<Payload> requestStream(Payload payload) {
		if (isSuccess()) {
			return peek().requestStream(payload);
		}
		return cachedRSocket.flatMapMany(rSocket -> rSocket.requestStream(payload));
	}

	@Override
	public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
		if (isSuccess()) {
			return peek().requestChannel(payloads);
		}
		return cachedRSocket.flatMapMany(rSocket -> rSocket.requestChannel(payloads));
	}

	@Override
	public Mono<Void> metadataPush(Payload payload) {
		if (isSuccess()) {
			return peek().metadataPush(payload);
		}
		return cachedRSocket.flatMap(rSocket -> rSocket.metadataPush(payload));
	}

	@Override
	public Mono<Void> onClose() {
		if (isSuccess()) {
			return peek().onClose();
		}
		return cachedRSocket.onErrorResume(throwable -> Mono.empty()).flatMap(RSocket::onClose);
	}

	@Override
	public void dispose() {
		if (isSuccess()) {
			peek().dispose();
		} else {
			cachedRSocket.dispose();
		}
	}

	@Override
	public double availability() {
		return isSuccess() ? peek().availability() : 1.0;
	}

	@Override
	public boolean isDisposed() {
		if (isSuccess()) {
			return peek().isDisposed();
		}
		return cachedRSocket.isDisposed();
	}

	private RSocket peek() {
		return cachedRSocket.peek();
	}

	private boolean isSuccess() {
		return cachedRSocket.isSuccess();
	}

}
