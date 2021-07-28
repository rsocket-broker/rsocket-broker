package io.rsocket.routing.http.bridge.core;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import io.rsocket.routing.client.spring.RoutingRSocketRequester;
import io.rsocket.routing.client.spring.RoutingRSocketRequesterBuilder;
import io.rsocket.routing.http.bridge.config.RSocketHttpBridgeProperties;
import io.rsocket.routing.http.bridge.support.SimpleClientTransportFactory;
import io.rsocket.routing.http.bridge.support.SimpleObjectProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.messaging.Message;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.messaging.support.GenericMessage;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Olga Maciaszek-Sharma
 */
class RequestResponseFunctionTests {

	private static final Duration VERIFY_TIMEOUT = Duration.ofMillis(100);

	RequestResponseFunction function;
	RoutingRSocketRequesterBuilder builder = mock(RoutingRSocketRequesterBuilder.class);
	RoutingRSocketRequester defaultRequester = mock(RoutingRSocketRequester.class);
	RoutingRSocketRequester.RoutingRequestSpec requestSpec = mock(RoutingRSocketRequester.RoutingRequestSpec.class);
	RSocketRequester.RetrieveSpec retrieveSpec = mock(RSocketRequester.RetrieveSpec.class);
	Message<Byte[]> outputMessage = new GenericMessage<>(buildPayload("output"));

	@BeforeEach
	void setup() {
		when(builder.transport(any())).thenReturn(null);
		when(requestSpec.address(any(Consumer.class))).thenReturn(requestSpec);
		when(retrieveSpec.retrieveMono(any(ParameterizedTypeReference.class)))
				.thenReturn(Mono.just(outputMessage));
		when(requestSpec.data(any(Byte[].class))).thenReturn(retrieveSpec);
		when(defaultRequester.route(eq("testRoute"))).thenReturn(requestSpec);
		function = new RequestResponseFunction(builder, defaultRequester, new SimpleObjectProvider<>(new SimpleClientTransportFactory()),
				new RSocketHttpBridgeProperties());
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

	private Byte[] buildPayload(String payloadString) {
		byte[] payload = payloadString
				.getBytes(StandardCharsets.UTF_8);
		Byte[] objectPayload = new Byte[payload.length];
		Arrays.setAll(objectPayload, n -> payload[n]);
		return objectPayload;
	}

	@Test
	void shouldReturnErrorWhenNoUriHeader() {
		Message<Byte[]> inputMessage = new GenericMessage<>(buildPayload("input"));
		StepVerifier.create(function.apply(Mono.just(inputMessage)))
				.expectError()
				.verify(VERIFY_TIMEOUT);
	}

}