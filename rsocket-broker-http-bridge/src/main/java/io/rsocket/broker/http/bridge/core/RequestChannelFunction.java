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

package io.rsocket.broker.http.bridge.core;

import java.net.URI;

import io.rsocket.broker.client.spring.BrokerRSocketRequester;
import io.rsocket.broker.common.spring.ClientTransportFactory;
import io.rsocket.broker.http.bridge.config.RSocketHttpBridgeProperties;
import reactor.core.publisher.Flux;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.messaging.Message;

import static io.rsocket.broker.common.WellKnownKey.SERVICE_NAME;
import static io.rsocket.broker.http.bridge.core.PathUtils.resolveAddress;
import static io.rsocket.broker.http.bridge.core.PathUtils.resolveRoute;
import static io.rsocket.broker.http.bridge.core.TagBuilder.buildTags;

/**
 * HTTP to RSocket Request-Channel mode function. Requests with path starting with {@code rc}
 * will be processed by this function.
 *
 * @author Olga Maciaszek-Sharma
 * @since 0.3.0
 */
public class RequestChannelFunction extends AbstractHttpRSocketFunction<Flux<Message<Byte[]>>, Flux<Message<Byte[]>>> {

	public RequestChannelFunction(BrokerRSocketRequester requester, ObjectProvider<ClientTransportFactory> transportFactories,
			RSocketHttpBridgeProperties properties) {
		super(requester, transportFactories, properties);
	}

	@Override
	public Flux<Message<Byte[]>> apply(Flux<Message<Byte[]>> messageFlux) {
		return messageFlux.flatMap(message -> {
			String uriString = (String) message.getHeaders().get("uri");
			if (uriString == null) {
				LOG.error("Uri cannot be null.");
				return Flux.error(new IllegalArgumentException("Uri cannot be null"));
			}
			URI uri = URI.create(uriString);
			String route = resolveRoute(uri);
			String serviceName = resolveAddress(uri);
			String tagString = (String) message.getHeaders()
					.get(properties.getTagsHeaderName());
			return requester
					.route(route)
					.address(builder -> builder.with(SERVICE_NAME, serviceName)
							.with(buildTags(tagString)))
					.data(Flux.just(message.getPayload()))
					.retrieveFlux(new ParameterizedTypeReference<Message<Byte[]>>() {
					})
					.timeout(timeout,
							Flux.defer(() -> {
								logTimeout(serviceName, route);
								// Flux.just("Request has timed out); ?
								return Flux
										.error(new IllegalArgumentException("Request has timed out."));
							}))
							.onErrorResume(error -> {
								logException(error, serviceName, route);
								return Flux.error(error);
							});
				}
		);
	}
}
