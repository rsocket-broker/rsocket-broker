package io.rsocket.routing.http.bridge.core;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.function.Consumer;

import io.rsocket.routing.client.spring.RoutingRSocketRequester;
import io.rsocket.routing.client.spring.RoutingRSocketRequesterBuilder;
import io.rsocket.routing.http.bridge.config.RSocketHttpBridgeProperties;
import org.junit.jupiter.api.BeforeEach;
import reactor.core.publisher.Mono;

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
abstract class AbstractFunctionTests {

	static final Duration VERIFY_TIMEOUT = Duration.ofMillis(100);

	protected RoutingRSocketRequesterBuilder builder = mock(RoutingRSocketRequesterBuilder.class);
	protected RoutingRSocketRequester defaultRequester = mock(RoutingRSocketRequester.class);
	protected RoutingRSocketRequester.RoutingRequestSpec requestSpec = mock(RoutingRSocketRequester.RoutingRequestSpec.class);
	protected RSocketRequester.RetrieveSpec retrieveSpec = mock(RSocketRequester.RetrieveSpec.class);
	protected Message<Byte[]> outputMessage = new GenericMessage<>(buildPayload("output"));
	protected RSocketHttpBridgeProperties properties = new RSocketHttpBridgeProperties();

	@BeforeEach
	void setup() {
		when(builder.transport(any())).thenReturn(null);
		when(requestSpec.address(any(Consumer.class))).thenReturn(requestSpec);
		when(retrieveSpec.retrieveMono(any(ParameterizedTypeReference.class)))
				.thenReturn(Mono.just(outputMessage));
		when(retrieveSpec.send()).thenReturn(Mono.empty());
		when(requestSpec.data(any(Byte[].class))).thenReturn(retrieveSpec);
		when(defaultRequester.route(eq("testRoute"))).thenReturn(requestSpec);
	}

	protected Byte[] buildPayload(String payloadString) {
		byte[] payload = payloadString
				.getBytes(StandardCharsets.UTF_8);
		Byte[] objectPayload = new Byte[payload.length];
		Arrays.setAll(objectPayload, n -> payload[n]);
		return objectPayload;
	}
}
