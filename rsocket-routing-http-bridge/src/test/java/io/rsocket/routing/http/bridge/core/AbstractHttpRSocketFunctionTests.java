/*
 * Copyright 2020-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
 * Tests for {@link AbstractHttpRSocketFunction}.
 *
 * @author Olga Maciaszek-Sharma
 * @since 0.3.0
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
