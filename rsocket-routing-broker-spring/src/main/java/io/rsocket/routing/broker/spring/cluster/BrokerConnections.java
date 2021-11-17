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

package io.rsocket.routing.broker.spring.cluster;

import io.rsocket.RSocket;
import reactor.core.publisher.Mono;

import org.springframework.messaging.rsocket.RSocketRequester;

/**
 * Maintains map of BrokerInfo to RSocketRequester of existing broker connections to current broker.
 * Used only for broker to broker communication.
 */
public class BrokerConnections extends AbstractConnections<RSocketRequester> {

	@Override
	protected Mono<RSocket> getRSocket(RSocketRequester requester) {
		if (requester.rsocketClient() != null) {
			return requester.rsocketClient().source();
		}
		return Mono.just(requester.rsocket());
	}
}
