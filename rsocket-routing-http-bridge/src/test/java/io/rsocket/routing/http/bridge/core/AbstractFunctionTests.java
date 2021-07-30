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

package io.rsocket.routing.http.bridge.core;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.function.Consumer;

import io.rsocket.routing.client.spring.RoutingRSocketRequester;
import io.rsocket.routing.http.bridge.config.RSocketHttpBridgeProperties;
import org.junit.jupiter.api.BeforeEach;

import org.springframework.messaging.Message;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.messaging.support.GenericMessage;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Base class for HTTP-RSocket function tests.
 *
 * @author Olga Maciaszek-Sharma
 * @since 0.3.0
 */
abstract class AbstractFunctionTests {

	static final Duration VERIFY_TIMEOUT = Duration.ofMillis(100);

	protected RoutingRSocketRequester requester = mock(RoutingRSocketRequester.class);
	protected RoutingRSocketRequester.RoutingRequestSpec requestSpec = mock(RoutingRSocketRequester.RoutingRequestSpec.class);
	protected RSocketRequester.RetrieveSpec retrieveSpec = mock(RSocketRequester.RetrieveSpec.class);
	protected Message<Byte[]> outputMessage = new GenericMessage<>(buildPayload("output"));
	protected RSocketHttpBridgeProperties properties = new RSocketHttpBridgeProperties();

	@BeforeEach
	void setup() {
		when(requestSpec.address(any(Consumer.class))).thenReturn(requestSpec);
		when(requestSpec.data(any())).thenReturn(retrieveSpec);
		when(requester.route(eq("testRoute"))).thenReturn(requestSpec);
	}

	protected Byte[] buildPayload(String payloadString) {
		byte[] payload = payloadString
				.getBytes(StandardCharsets.UTF_8);
		Byte[] objectPayload = new Byte[payload.length];
		Arrays.setAll(objectPayload, n -> payload[n]);
		return objectPayload;
	}
}
