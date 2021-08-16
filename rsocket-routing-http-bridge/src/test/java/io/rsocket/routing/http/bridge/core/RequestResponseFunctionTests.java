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

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import io.rsocket.routing.http.bridge.support.SimpleClientTransportFactory;
import io.rsocket.routing.http.bridge.support.SimpleObjectProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link RequestResponseFunction}.
 *
 * @author Olga Maciaszek-Sharma
 * @since 0.3.0
 */
class RequestResponseFunctionTests extends AbstractFunctionTests {

	private RequestResponseFunction function;

	@BeforeEach
	void setup() {
		when(retrieveSpec.retrieveMono(any(ParameterizedTypeReference.class)))
				.thenReturn(Mono.just(outputMessage));
		super.setup();
		function = new RequestResponseFunction(requester, new SimpleObjectProvider<>(new SimpleClientTransportFactory()),
				properties);
	}

	@Test
	void shouldReturnResponse() {
		Map<String, Object> headers = new HashMap<>();
		headers.put("uri", "http://test.org/testAddress/testRoute");
		Message<Byte[]> inputMessage = new GenericMessage<>(buildPayload("input"), headers);
		StepVerifier.create(function.apply(Mono.just(inputMessage)))
				.expectSubscription()
				.expectNext(outputMessage)
				.thenCancel()
				.verify(VERIFY_TIMEOUT);
	}

	@Test
	void shouldReturnErrorWhenNoUriHeader() {
		Message<Byte[]> inputMessage = new GenericMessage<>(buildPayload("input"));
		StepVerifier.create(function.apply(Mono.just(inputMessage)))
				.expectError()
				.verify(VERIFY_TIMEOUT);
	}

	@Test
	void shouldTimeout() {
		StepVerifier.withVirtualTime(() -> {
			properties.setTimeout(Duration.ofMillis(1));
			function = new RequestResponseFunction(requester, new SimpleObjectProvider<>(new SimpleClientTransportFactory()),
					properties);
			Map<String, Object> headers = new HashMap<>();
			headers.put("uri", "http://test.org/testAddress/testRoute");
			Message<Byte[]> inputMessage = new GenericMessage<>(buildPayload("input"), headers);
			return function
					.apply(Mono.just(inputMessage))
					.delayElement(Duration.ofMillis(2));
		}).expectSubscription()
				.verifyTimeout(Duration.ofMillis(1));
	}

}