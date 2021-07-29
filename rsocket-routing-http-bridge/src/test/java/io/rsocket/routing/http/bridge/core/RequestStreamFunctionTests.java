package io.rsocket.routing.http.bridge.core;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import io.rsocket.routing.http.bridge.support.SimpleClientTransportFactory;
import io.rsocket.routing.http.bridge.support.SimpleObjectProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * @author Olga Maciaszek-Sharma
 */
public class RequestStreamFunctionTests extends AbstractFunctionTests {

	private RequestStreamFunction function;

	@BeforeEach
	void setup() {
		when(retrieveSpec.retrieveFlux(any(ParameterizedTypeReference.class)))
				.thenReturn(Flux.just(outputMessage));
		super.setup();
		when(retrieveSpec.send()).thenReturn(Mono.empty());
		function = new RequestStreamFunction(builder, defaultRequester, new SimpleObjectProvider<>(new SimpleClientTransportFactory()),
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
			function = new RequestStreamFunction(builder, defaultRequester, new SimpleObjectProvider<>(new SimpleClientTransportFactory()),
					properties);
			Map<String, Object> headers = new HashMap<>();
			headers.put("uri", "http://test.org/testAddress/testRoute");
			Message<Byte[]> inputMessage = new GenericMessage<>(buildPayload("input"), headers);
			return function
					.apply(Mono.just(inputMessage))
					.delaySequence(Duration.ofMillis(2));
		}).expectSubscription()
				.verifyTimeout(Duration.ofMillis(1));
	}
}
