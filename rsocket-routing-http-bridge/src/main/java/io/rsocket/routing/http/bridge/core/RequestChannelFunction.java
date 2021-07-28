package io.rsocket.routing.http.bridge.core;

import java.net.URI;

import io.rsocket.routing.client.spring.RoutingRSocketRequester;
import io.rsocket.routing.client.spring.RoutingRSocketRequesterBuilder;
import io.rsocket.routing.common.spring.ClientTransportFactory;
import io.rsocket.routing.http.bridge.config.RSocketHttpBridgeProperties;
import reactor.core.publisher.Flux;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.messaging.Message;

import static io.rsocket.routing.common.WellKnownKey.SERVICE_NAME;
import static io.rsocket.routing.http.bridge.core.PathUtils.resolveAddress;
import static io.rsocket.routing.http.bridge.core.PathUtils.resolveRoute;
import static io.rsocket.routing.http.bridge.core.TagBuilder.buildTags;

/**
 * @author Olga Maciaszek-Sharma
 */
public class RequestChannelFunction extends AbstractHttpRSocketFunction<Flux<Message<Byte[]>>, Flux<Message<Byte[]>>> {

	public RequestChannelFunction(RoutingRSocketRequesterBuilder requesterBuilder, RoutingRSocketRequester defaultRequester, ObjectProvider<ClientTransportFactory> transportFactories,
			RSocketHttpBridgeProperties properties) {
		super(requesterBuilder, defaultRequester, transportFactories, properties);
	}

	@Override
	public Flux<Message<Byte[]>> apply(Flux<Message<Byte[]>> messageFlux) {
		return messageFlux.flatMap(message -> {
			String uriString = (String) message.getHeaders().get("uri");
			if (uriString == null) {
				LOG.error("Uri cannot be null.");
				return Flux.error(new IllegalArgumentException("Uri cannot be null"));
			}
			URI uri = URI.create(uriString);
			String route = resolveRoute(uri);
			String serviceName = resolveAddress(uri);
			String tagString = (String) message.getHeaders()
					.get(properties.getTagsHeaderName());
			String brokerHeader = (String) message.getHeaders()
					.get(properties.getBrokerDataHeaderName());
			return getRequester(brokerHeader)
					.route(route)
					.address(builder -> builder.with(SERVICE_NAME, serviceName)
							.with(buildTags(tagString)))
					.data(Flux.just(message.getPayload()))
					.retrieveFlux(new ParameterizedTypeReference<Message<Byte[]>>() {
					})
					.timeout(timeout,
							Flux.defer(() -> {
								logTimeout(serviceName, route);
								// Flux.just("Request has timed out); ?
								return Flux
										.error(new IllegalArgumentException("Request has timed out."));
							}))
							.onErrorResume(error -> {
								logException(error, serviceName, route);
								return Flux.error(error);
							});
				}
		);
	}
}
