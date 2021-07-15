package io.rsocket.routing.http.bridge.core;

import java.net.URI;

import io.rsocket.routing.client.spring.RoutingRSocketRequester;
import io.rsocket.routing.http.bridge.config.RSocketHttpBridgeProperties;
import reactor.core.publisher.Mono;

import org.springframework.messaging.Message;

import static io.rsocket.routing.common.WellKnownKey.SERVICE_NAME;
import static io.rsocket.routing.http.bridge.core.PathUtils.resolveAddress;
import static io.rsocket.routing.http.bridge.core.PathUtils.resolveRoute;

/**
 * @author Olga Maciaszek-Sharma
 */
public class FireAndForgetFunction extends AbstractHttpRSocketFunction<Mono<Message<String>>, Mono<Void>> {

	public FireAndForgetFunction(RoutingRSocketRequester requester, RSocketHttpBridgeProperties properties) {
		super(requester, properties);
	}

	@Override
	public Mono<Void> apply(Mono<Message<String>> messageMono) {
		return messageMono.flatMap(message -> {
			String uriString = (String) message.getHeaders().get("uri");
			if (uriString == null) {
				LOG.error("Uri cannot be null.");
				return Mono.error(new IllegalArgumentException("Uri cannot be null"));
			}
			URI uri = URI.create(uriString);
			String route = resolveRoute(uri);
			String serviceName = resolveAddress(uri);
			String tagString = (String) message.getHeaders()
					.get(properties.getTagHeaderName());
			return requester
					// TODO: handle different protocols
					.route(route)
					.address(builder -> builder.with(SERVICE_NAME, serviceName)
							.with(buildTags(tagString)))
					.data(message.getPayload())
					.send()
					.timeout(timeout,
							Mono.defer(() -> {
								logTimeout(serviceName, route);
								// Mono.just("Request has timed out); ?
								return Mono
										.error(new IllegalArgumentException("Request has timed out."));
							}))
							.onErrorResume(error -> {
								logException(error, serviceName, route);
								return Mono.error(error);
							});
				}
		);
	}
}
