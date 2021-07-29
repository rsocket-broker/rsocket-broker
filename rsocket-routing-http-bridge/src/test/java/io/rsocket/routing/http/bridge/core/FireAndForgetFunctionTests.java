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

/**
 * @author Olga Maciaszek-Sharma
 */
class FireAndForgetFunctionTests extends AbstractFunctionTests {

	private FireAndForgetFunction function;

	@BeforeEach
	void setup() {
		super.setup();
		function = new FireAndForgetFunction(builder, defaultRequester, new SimpleObjectProvider<>(new SimpleClientTransportFactory()),
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