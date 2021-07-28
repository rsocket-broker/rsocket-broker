package io.rsocket.routing.http.bridge.support;

import io.rsocket.routing.common.spring.ClientTransportFactory;
import io.rsocket.routing.common.spring.TransportProperties;
import io.rsocket.transport.ClientTransport;

/**
 * @author Olga Maciaszek-Sharma
 */
public class SimpleClientTransportFactory implements ClientTransportFactory {

	@Override
	public boolean supports(TransportProperties broker) {
		return true;
	}

	@Override
	public ClientTransport create(TransportProperties broker) {
		return new SimpleClientTransport(broker);
	}
}
