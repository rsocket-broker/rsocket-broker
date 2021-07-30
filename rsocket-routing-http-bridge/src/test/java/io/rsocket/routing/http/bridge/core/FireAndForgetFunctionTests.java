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

import java.util.HashMap;
import java.util.Map;

import io.rsocket.routing.http.bridge.support.SimpleClientTransportFactory;
import io.rsocket.routing.http.bridge.support.SimpleObjectProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;

import static org.mockito.Mockito.when;

/**
 * Tests for {@link FireAndForgetFunction}.
 *
 * @author Olga Maciaszek-Sharma
 * @since 0.3.0
 */
class FireAndForgetFunctionTests extends AbstractFunctionTests {

	private FireAndForgetFunction function;

	@BeforeEach
	void setup() {
		when(retrieveSpec.send()).thenReturn(Mono.empty());
		super.setup();
		function = new FireAndForgetFunction(requester, new SimpleObjectProvider<>(new SimpleClientTransportFactory()),
				properties);
	}

	@Test
	void shouldReturnSendAndCompleteStream() {
		Map<String, Object> headers = new HashMap<>();
		headers.put("uri", "http://test.org/testAddress/testRoute");
		Message<Byte[]> inputMessage = new GenericMessage<>(buildPayload("input"), headers);
		StepVerifier.create(function.apply(Mono.just(inputMessage)))
				.expectSubscription()
				.expectNextCount(0)
				.verifyComplete();
	}

	@Test
	void shouldReturnErrorWhenNoUriHeader() {
		Message<Byte[]> inputMessage = new GenericMessage<>(buildPayload("input"));
		StepVerifier.create(function.apply(Mono.just(inputMessage)))
				.expectError()
				.verify(VERIFY_TIMEOUT);
	}

}