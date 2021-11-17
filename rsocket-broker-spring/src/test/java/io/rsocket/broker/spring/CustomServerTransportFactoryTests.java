/*
 * Copyright 2021 the original author or authors.
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

package io.rsocket.broker.spring;

import java.net.InetSocketAddress;
import java.net.URI;

import io.rsocket.Closeable;
import io.rsocket.SocketAcceptor;
import io.rsocket.broker.spring.BrokerAutoConfiguration.BrokerRSocketServerBootstrap;
import io.rsocket.broker.spring.BrokerProperties;
import io.rsocket.broker.spring.ServerTransportFactory;
import io.rsocket.broker.common.Id;
import io.rsocket.transport.local.LocalServerTransport;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.rsocket.server.RSocketServer;
import org.springframework.boot.rsocket.server.RSocketServerException;
import org.springframework.boot.rsocket.server.RSocketServerFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {"io.rsocket.broker.broker-id=00000000-0000-0000-0000-000000000008",
		"io.rsocket.broker.uri=local://mylocal",
		"io.rsocket.broker.cluster.uri=local://myclusterlocal"})
public class CustomServerTransportFactoryTests {

	@Autowired
	BrokerProperties properties;

	@Autowired
	ObjectProvider<BrokerRSocketServerBootstrap> bootstrapProvider;

	@Test
	public void customPropertiesAreSet() {
		assertThat(properties.getBrokerId()).isEqualTo(Id.from("00000000-0000-0000-0000-000000000008"));
		assertThat(properties.getUri()).isNotNull().hasScheme("local").hasHost("mylocal");
		assertThat(properties.getCluster().getUri()).isNotNull().hasScheme("local").hasHost("myclusterlocal");
		bootstrapProvider.stream()
				.filter(b -> b.getServerFactory() instanceof MyLocalRSocketServerFactory).findFirst()
				.orElseThrow(() -> new IllegalStateException("no MyLocalRSocketServerFactory found"));
	}

	protected static class MyLocalRSocketServerFactory implements RSocketServerFactory {
		private final URI uri;

		public MyLocalRSocketServerFactory(URI uri) {
			this.uri = uri;
		}

		@Override
		public RSocketServer create(SocketAcceptor socketAcceptor) {
			io.rsocket.core.RSocketServer server = io.rsocket.core.RSocketServer
					.create(socketAcceptor);
			String name = uri.getHost();
			Mono<Closeable> starter = server.bind(LocalServerTransport.create(name));

			return new RSocketServer() {
				private Closeable channel;

				@Override
				public void start() throws RSocketServerException {
					channel = starter.block();
					Thread awaitThread = new Thread(() -> channel.onClose().block(), "rsocket");
					awaitThread.setContextClassLoader(getClass().getClassLoader());
					awaitThread.setDaemon(false);
					awaitThread.start();
				}

				@Override
				public void stop() throws RSocketServerException {
					if (this.channel != null) {
						this.channel.dispose();
						this.channel = null;
					}
				}

				@Override
				public InetSocketAddress address() {
					return null;
				}
			};
		}
	}

	@SpringBootConfiguration
	@EnableAutoConfiguration
	protected static class TestConfig {

		@Bean
		@Order(Ordered.HIGHEST_PRECEDENCE)
		ServerTransportFactory customServerTransportFactory() {
			return new ServerTransportFactory() {
				@Override
				public boolean supports(URI uri) {
					return uri.getScheme().equalsIgnoreCase("local");
				}

				@Override
				public RSocketServerFactory create(URI uri) {
					return new MyLocalRSocketServerFactory(uri);
				}
			};
		}

	}
}
