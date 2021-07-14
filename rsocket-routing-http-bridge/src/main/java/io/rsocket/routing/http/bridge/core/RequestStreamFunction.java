package io.rsocket.routing.http.bridge.core;

import java.net.URI;

import io.rsocket.routing.client.spring.RoutingRSocketRequester;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.messaging.Message;

import static io.rsocket.routing.http.bridge.core.PathUtils.resolveAddress;
import static io.rsocket.routing.http.bridge.core.PathUtils.resolveRoute;

/**
 * @author Olga Maciaszek-Sharma
 */
public class RequestStreamFunction extends AbstractHttpRSocketFunction<Mono<Message<String>>, Flux<Message<String>>> {

	private final RoutingRSocketRequester requester;

	public RequestStreamFunction(RoutingRSocketRequester requester) {
		this.requester = requester;
	}

	@Override
	public Flux<Message<String>> apply(Mono<Message<String>> messageMono) {
		return Flux.from(messageMono).flatMap(message -> {
					String uriString = (String) message.getHeaders().get("uri");
					if (uriString == null) {
						LOG.error("Uri cannot be null.");
						return Flux.error(new IllegalArgumentException("Uri cannot be null"));
					}
					URI uri = URI.create(uriString);
					String route = resolveRoute(uri);
					String address = resolveAddress(uri);
					return requester
							// TODO: handle different protocols
							.route(route)
							.address(address)
							.data(message.getPayload())
							.retrieveFlux(new ParameterizedTypeReference<Message<String>>() {
							})
							.timeout(timeout,
									Flux.defer(() -> {
										logTimeout(address, route);
										// Mono.just("Request has timed out); ?
										return Flux
												.error(new IllegalArgumentException("Request has timed out."));
									}))
							.onErrorResume(error -> {
								logException(error, address, route);
								return Flux.error(error);
							});
				}
		);
	}
}
