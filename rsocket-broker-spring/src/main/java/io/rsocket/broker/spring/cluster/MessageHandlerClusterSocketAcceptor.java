/*
 * Copyright 2021 the original author or authors.
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

package io.rsocket.broker.spring.cluster;

import io.rsocket.ConnectionSetupPayload;
import io.rsocket.RSocket;
import io.rsocket.broker.acceptor.ClusterSocketAcceptor;
import reactor.core.publisher.Mono;

import org.springframework.messaging.rsocket.annotation.support.RSocketMessageHandler;

public class MessageHandlerClusterSocketAcceptor extends ClusterSocketAcceptor {

	private final RSocketMessageHandler messageHandler;

	public MessageHandlerClusterSocketAcceptor(RSocketMessageHandler messageHandler) {
		this.messageHandler = messageHandler;
	}

	@Override
	public Mono<RSocket> accept(ConnectionSetupPayload setup, RSocket sendingSocket) {
		return this.messageHandler.responder().accept(setup, sendingSocket);
	}
}
