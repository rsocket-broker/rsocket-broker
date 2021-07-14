package io.rsocket.routing.http.bridge.core;

import java.net.URI;

import io.rsocket.routing.client.spring.RoutingRSocketRequester;
import reactor.core.publisher.Mono;

import org.springframework.messaging.Message;

import static io.rsocket.routing.http.bridge.core.PathUtils.resolveAddress;
import static io.rsocket.routing.http.bridge.core.PathUtils.resolveRoute;

/**
 * @author Olga Maciaszek-Sharma
 */
public class FireAndForgetFunction extends AbstractHttpRSocketFunction<Mono<Message<String>>, Mono<Void>> {

	private final RoutingRSocketRequester requester;

	public FireAndForgetFunction(RoutingRSocketRequester requester) {
		this.requester = requester;
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
					String address = resolveAddress(uri);
					return requester
							// TODO: handle different protocols
							.route(route)
							.address(address)
							.data(message.getPayload())
							.send()
							.timeout(timeout,
									Mono.defer(() -> {
										logTimeout(address, route);
										// Mono.just("Request has timed out); ?
										return Mono
												.error(new IllegalArgumentException("Request has timed out."));
									}))
							.onErrorResume(error -> {
								logException(error, address, route);
								return Mono.error(error);
							});
				}
		);
	}
}
