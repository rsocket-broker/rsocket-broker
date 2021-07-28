package io.rsocket.routing.http.bridge.support;

import io.rsocket.DuplexConnection;
import io.rsocket.routing.common.spring.TransportProperties;
import io.rsocket.transport.ClientTransport;
import reactor.core.publisher.Mono;

/**
 * @author Olga Maciaszek-Sharma
 */
public class SimpleClientTransport implements ClientTransport {

	private final TransportProperties broker;

	public SimpleClientTransport(TransportProperties broker) {
		this.broker = broker;
	}


	@Override
	public Mono<DuplexConnection> connect() {
		return Mono.empty();
	}

	public TransportProperties getBroker() {
		return broker;
	}
}
