/*
 * Copyright 2020-2021 the original author or authors.
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

package io.rsocket.routing.http.bridge.support;

import io.rsocket.DuplexConnection;
import io.rsocket.routing.common.spring.TransportProperties;
import io.rsocket.transport.ClientTransport;
import reactor.core.publisher.Mono;

/**
 * Test-specific implementation of {@link ClientTransport}.
 *
 * @author Olga Maciaszek-Sharma
 * @since 0.3.0
 */
public class SimpleClientTransport implements ClientTransport {

	private final TransportProperties broker;

	public SimpleClientTransport(TransportProperties broker) {
		this.broker = broker;
	}


	@Override
	public Mono<DuplexConnection> connect() {
		return Mono.empty();
	}

	public TransportProperties getBroker() {
		return broker;
	}
}
