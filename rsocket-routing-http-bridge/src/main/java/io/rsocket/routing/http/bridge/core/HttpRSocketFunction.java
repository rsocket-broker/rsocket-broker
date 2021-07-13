package io.rsocket.routing.http.bridge.core;

import java.net.URI;
import java.time.Duration;
import java.util.function.Function;

import io.rsocket.routing.client.spring.RoutingRSocketRequester;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import reactor.core.publisher.Mono;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.messaging.Message;

import static io.rsocket.routing.http.bridge.core.PathUtils.resolveAddress;
import static io.rsocket.routing.http.bridge.core.PathUtils.resolveRoute;

/**
 * @author Olga Maciaszek-Sharma
 */
public class HttpRSocketFunction implements Function<Mono<Message<String>>, Mono<Message<String>>> {

	private static final Log LOG = LogFactory
			.getLog(HttpRSocketFunction.class);

	private final RoutingRSocketRequester requester;

	// TODO: get from properties
	private Duration timeout;

	public HttpRSocketFunction(RoutingRSocketRequester requester) {
		this.requester = requester;
	}

	@Override
	public Mono<Message<String>> apply(Mono<Message<String>> messageMono) {
		return messageMono.flatMap(message -> {
					String uriString = (String) message.getHeaders().get("uri");
					if (uriString == null) {
						LOG.error("Uri cannot be null.");
						return Mono
								.error(new IllegalArgumentException("Uri cannot be null"));
					}
					URI uri = URI.create(uriString);
					String route = resolveRoute(uri);
					String address = resolveAddress(uri);
					timeout = Duration.ofSeconds(30);
					return requester
							// TODO: handle different protocols
							.route(route)
							.address(address)
							.data(message.getPayload())
							.retrieveMono(new ParameterizedTypeReference<Message<String>>() {
							})
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


	private void logTimeout(String address, String route) {
		if (LOG.isDebugEnabled()) {
			LOG.debug(String
					.format("Timeout occurred while retrieving RSocket response from address: %s, route: %s. Response was not retrieved within %s", address, route, timeout));
		}
	}

	private void logException(Throwable error, String address, String route) {
		if (LOG.isErrorEnabled())
			LOG.error(String
					.format("Exception occurred while retrieving RSocket response from address: %s, route: %s", address, route), error);
	}
}
