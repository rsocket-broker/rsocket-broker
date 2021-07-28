package io.rsocket.routing.http.bridge.core;

import io.rsocket.routing.client.spring.RoutingRSocketRequesterBuilder;
import io.rsocket.routing.common.spring.TransportProperties;
import io.rsocket.routing.http.bridge.config.RSocketHttpBridgeProperties;
import io.rsocket.routing.http.bridge.support.SimpleClientTransport;
import io.rsocket.routing.http.bridge.support.SimpleClientTransportFactory;
import io.rsocket.routing.http.bridge.support.SimpleObjectProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import reactor.core.publisher.Mono;

import org.springframework.messaging.Message;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


/**
 * @author Olga Maciaszek-Sharma
 */
class AbstractHttpRSocketFunctionTests {

	RoutingRSocketRequesterBuilder builder;
	TestHttpRSocketFunction httpRSocketFunction;

	@BeforeEach
	void setup() {
		builder = mock(RoutingRSocketRequesterBuilder.class);
		when(builder.transport(any())).thenReturn(null);
		httpRSocketFunction = new TestHttpRSocketFunction(builder);
	}


	@Test
	void shouldBuildRequesterWithTcpBrokerFromHeader() {
		ArgumentCaptor<SimpleClientTransport> transportCaptor = ArgumentCaptor
				.forClass(SimpleClientTransport.class);
		String brokerHeader = "transport=tcp,host=test.org,port=8080";

		try {
			httpRSocketFunction.getRequester(brokerHeader);
		}
		catch (NullPointerException ignored) {
		}

		verify(builder).transport(transportCaptor.capture());
		SimpleClientTransport transport = transportCaptor.getValue();
		TransportProperties broker = transport.getBroker();
		assertThat(broker.getTcp()).isNotNull();
		TransportProperties.TcpProperties tcpBroker = broker.getTcp();
		assertThat(tcpBroker.getHost()).isEqualTo("test.org");
		assertThat(tcpBroker.getPort()).isEqualTo(8080);
	}

	@Test
	void shouldUseTcpWhenTransportNotResolved() {
		ArgumentCaptor<SimpleClientTransport> transportCaptor = ArgumentCaptor
				.forClass(SimpleClientTransport.class);
		String brokerHeader = "transport=xxx,host=test.org,port=8080";

		try {
			httpRSocketFunction.getRequester(brokerHeader);
		}
		catch (NullPointerException ignored) {
		}

		verify(builder).transport(transportCaptor.capture());
		SimpleClientTransport transport = transportCaptor.getValue();
		TransportProperties broker = transport.getBroker();
		assertThat(broker.getTcp()).isNotNull();
		TransportProperties.TcpProperties tcpBroker = broker.getTcp();
		assertThat(tcpBroker.getHost()).isEqualTo("test.org");
		assertThat(tcpBroker.getPort()).isEqualTo(8080);
	}

	@Test
	void shouldUseTcpWhenNoTransport() {
		ArgumentCaptor<SimpleClientTransport> transportCaptor = ArgumentCaptor
				.forClass(SimpleClientTransport.class);
		String brokerHeader = "host=test.org,port=8080";

		try {
			httpRSocketFunction.getRequester(brokerHeader);
		}
		catch (NullPointerException ignored) {
		}

		verify(builder).transport(transportCaptor.capture());
		SimpleClientTransport transport = transportCaptor.getValue();
		TransportProperties broker = transport.getBroker();
		assertThat(broker.getTcp()).isNotNull();
		TransportProperties.TcpProperties tcpBroker = broker.getTcp();
		assertThat(tcpBroker.getHost()).isEqualTo("test.org");
		assertThat(tcpBroker.getPort()).isEqualTo(8080);
	}

	@Test
	void shouldBuildRequesterWithWebsocketBrokerFromHeader() {
		ArgumentCaptor<SimpleClientTransport> transportCaptor = ArgumentCaptor
				.forClass(SimpleClientTransport.class);
		String brokerHeader = "transport=wEBsocket,host=test.org,port=80,mapping-path=test";

		try {
			httpRSocketFunction.getRequester(brokerHeader);
		}
		catch (NullPointerException ignored) {
		}

		verify(builder).transport(transportCaptor.capture());
		SimpleClientTransport transport = transportCaptor.getValue();
		TransportProperties broker = transport.getBroker();
		assertThat(broker.getWebsocket()).isNotNull();
		TransportProperties.WebsocketProperties websocketBroker = broker.getWebsocket();
		assertThat(websocketBroker.getHost()).isEqualTo("test.org");
		assertThat(websocketBroker.getPort()).isEqualTo(80);
		assertThat(websocketBroker.getMappingPath()).isEqualTo("test");
	}

	@Test
	void shouldReturnDefaultRequesterWhenNullBrokerHeader() {
		ArgumentCaptor<SimpleClientTransport> transportCaptor = ArgumentCaptor
				.forClass(SimpleClientTransport.class);
		String brokerHeader = null;

		try {
			httpRSocketFunction.getRequester(brokerHeader);
		}
		catch (NullPointerException ignored) {
		}

		verify(builder, times(0)).transport(transportCaptor.capture());
	}


	static class TestHttpRSocketFunction extends AbstractHttpRSocketFunction<Mono<Message<Byte[]>>, Mono<Message<Byte[]>>> {

		TestHttpRSocketFunction(RoutingRSocketRequesterBuilder builder) {
			super(builder, null, new SimpleObjectProvider<>(new SimpleClientTransportFactory()), new RSocketHttpBridgeProperties());
		}

		@Override
		public Mono<Message<Byte[]>> apply(Mono<Message<Byte[]>> messageMono) {
			return messageMono;
		}
	}

}
