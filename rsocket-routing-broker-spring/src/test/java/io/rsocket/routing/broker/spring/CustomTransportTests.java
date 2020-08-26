/*
 * Copyright 2020 the original author or authors.
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

package io.rsocket.routing.broker.spring;

import java.net.InetSocketAddress;

import io.rsocket.Closeable;
import io.rsocket.SocketAcceptor;
import io.rsocket.routing.broker.config.BrokerProperties;
import io.rsocket.routing.broker.config.ClusterBrokerProperties;
import io.rsocket.routing.broker.config.TransportProperties;
import io.rsocket.routing.broker.spring.BrokerAutoConfiguration.BrokerRSocketServerBootstrap;
import io.rsocket.routing.common.Id;
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
import static org.assertj.core.api.Assertions.entry;

@SpringBootTest(properties = {"io.rsocket.routing.broker.broker-id=00000000-0000-0000-0000-000000000008",
		"io.rsocket.routing.broker.custom.type=local",
		"io.rsocket.routing.broker.custom.args.name=mylocal",
		"io.rsocket.routing.broker.cluster.custom.type=local",
		"io.rsocket.routing.broker.cluster.custom.args.name=myclusterlocal"})
public class CustomTransportTests {

	@Autowired
	BrokerProperties properties;

	@Autowired
	ClusterBrokerProperties clusterProperties;

	@Autowired
	ObjectProvider<BrokerRSocketServerBootstrap> bootstrapProvider;

	@Test
	public void customPropertiesAreSet() {
		assertThat(properties.getBrokerId()).isEqualTo(Id.from("00000000-0000-0000-0000-000000000008"));
		assertThat(properties.getCustom()).isNotNull();
		assertThat(properties.getCustom().getArgs()).containsOnly(entry("name", "mylocal"));
		assertThat(clusterProperties.getCustom()).isNotNull();
		assertThat(clusterProperties.getCustom().getArgs()).containsOnly(entry("name", "myclusterlocal"));
		bootstrapProvider.stream()
				.filter(b -> b.getServerFactory() instanceof MyLocalRSocketServerFactory).findFirst()
				.orElseThrow(() -> new IllegalStateException("no MyLocalRSocketServerFactory found"));
	}

	protected static class MyLocalRSocketServerFactory implements RSocketServerFactory {
		private final TransportProperties properties;

		public MyLocalRSocketServerFactory(TransportProperties properties) {
			this.properties = properties;
		}

		@Override
		public RSocketServer create(SocketAcceptor socketAcceptor) {
			io.rsocket.core.RSocketServer server = io.rsocket.core.RSocketServer
					.create(socketAcceptor);
			String name = properties.getCustom().getArgs().get("name");
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
					// FIXME: see https://github.com/spring-projects/spring-boot/pull/23084
					return InetSocketAddress.createUnresolved("localhost", 0);
					//return null;
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
				public boolean supports(TransportProperties properties) {
					return properties.hasCustomTransport() && properties.getCustom().getType().equals("local");
				}

				@Override
				public RSocketServerFactory create(TransportProperties properties) {
					return new MyLocalRSocketServerFactory(properties);
				}
			};
		}

	}
}
