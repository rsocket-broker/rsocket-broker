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

package io.rsocket.routing.broker.spring.test;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.metadata.CompositeMetadataCodec;
import io.rsocket.metadata.TaggingMetadataCodec;
import io.rsocket.metadata.WellKnownMimeType;
import io.rsocket.routing.client.RoutingRSocketClient;
import io.rsocket.routing.client.RoutingRSocketConnector;
import io.rsocket.routing.common.Id;
import io.rsocket.routing.common.Tags;
import io.rsocket.routing.common.WellKnownKey;
import io.rsocket.routing.common.spring.MimeTypes;
import io.rsocket.routing.frames.AddressFlyweight;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.DefaultPayload;
import io.rsocket.util.RSocketProxy;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.messaging.rsocket.RSocketStrategies;

import static io.netty.buffer.Unpooled.EMPTY_BUFFER;
import static io.rsocket.routing.common.spring.MimeTypes.COMPOSITE_MIME_TYPE;

@SpringBootConfiguration
@EnableAutoConfiguration
public class PingPongApp {

	@Bean
	@ConditionalOnProperty(name = "ping.one.enabled", matchIfMissing = true)
	public Ping ping1() {
		return new Ping(1L);
	}

	@Bean
	@ConditionalOnProperty("ping.two.enabled")
	public Ping ping2() {
		return new Ping(2L);
	}

	@Bean
	@ConditionalOnProperty(name = "pong.enabled", matchIfMissing = true)
	public Pong pong() {
		return new Pong();
	}

	/*@Bean
	public ClusterClient clusterClient() {
		return new ClusterClient();
	}*/

	public static void main(String[] args) {
		Hooks.onOperatorDebug();
		SpringApplication.run(PingPongApp.class, args);
	}

	static String reply(String in) {
		if (in.length() > 4) {
			in = in.substring(0, 4);
		}
		switch (in.toLowerCase()) {
		case "ping":
			return "pong";
		case "pong":
			return "ping";
		default:
			throw new IllegalArgumentException("Value must be ping or pong, not " + in);
		}
	}

	private static void startDaemonThread(String name, MonoProcessor<Void> onClose) {
		Thread awaitThread =
				new Thread(name + "-thread") {

					@Override
					public void run() {
						onClose.block();
					}
				};
		awaitThread.setContextClassLoader(PingPongApp.class.getClassLoader());
		awaitThread.setDaemon(false);
		awaitThread.start();
	}

	public static class Ping
			implements Ordered, ApplicationListener<ApplicationReadyEvent> {

		private Logger logger = LoggerFactory.getLogger(getClass());

		private final Id id;

		private final AtomicInteger pongsReceived = new AtomicInteger();

		public Ping(Long id) {
			this.id = new Id(0, id);
		}

		@Override
		public int getOrder() {
			return 0;
		}

		public void onApplicationEventx(ApplicationReadyEvent event) {
			logger.info("Skipping Ping {}", id);
		}

		public void onApplicationEvent(ApplicationReadyEvent event) {
			logger.info("Starting Ping {}", id);
			ConfigurableEnvironment env = event.getApplicationContext().getEnvironment();
			Integer take = env.getProperty("ping.take", Integer.class, null);
			String host = env.getProperty("ping.broker.host", String.class, "localhost");
			Integer port = env.getProperty("ping.broker.port", Integer.class, 8001);

			logger.debug("ping.take: {}", take);

			RoutingRSocketClient client = RoutingRSocketConnector.create()
					.routeId(id)
					.serviceName("ping")
					.toRSocketClient(TcpClientTransport.create(host, port));

			Flux<? extends String> pongFlux = Flux.from(doPing(take, client));

			boolean subscribe = env.getProperty("ping.subscribe", Boolean.class, true);

			if (subscribe) {
				pongFlux.subscribe();
			}

			MonoProcessor<Void> onClose = MonoProcessor.create();

			startDaemonThread("ping" + id, onClose);
		}

		//Publisher<? extends String> doPing(Integer take, RSocket socket) {
		Publisher<? extends String> doPing(Integer take, RoutingRSocketClient client) {
			Flux<String> pong = client
					.requestChannel(Flux.interval(Duration.ofSeconds(1)).map(i -> {
						ByteBuf data = ByteBufUtil.writeUtf8(ByteBufAllocator.DEFAULT,
								"ping" + id);
						CompositeByteBuf compositeMetadata = client.allocator().compositeBuffer();
						//client.encodeAddressMetadata(compositeMetadata, "pong");
						client.encodeAddressMetadata(compositeMetadata, tags -> tags.with(WellKnownKey.SERVICE_NAME, "pong"));
						logger.debug("Sending ping" + id);
						return DefaultPayload.create(data, compositeMetadata);
						// onBackpressure is needed in case pong is not available yet
					}).log("doPing")
							.onBackpressureDrop(payload -> logger
									.debug("Dropped payload {}", payload.getDataUtf8())))
					.map(Payload::getDataUtf8).doOnNext(str -> {
						int received = pongsReceived.incrementAndGet();
						logger.info("received {}({}) in Ping {}", str, received, id);
					}).doFinally(signal -> client.dispose());
			if (take != null) {
				return pong.take(take);
			}
			return pong;
		}

	}

	public static class Pong
			implements Ordered, ApplicationListener<ApplicationReadyEvent> {

		private final Id routeId = new Id(0, 3L);
		private Logger logger = LoggerFactory.getLogger(getClass());

		private final AtomicInteger pingsReceived = new AtomicInteger();
		private RoutingRSocketClient rSocketClient;

		@Override
		public int getOrder() {
			return 1;
		}

		public void onApplicationEventx(ApplicationReadyEvent event) {
			logger.info("Skipping Pong");
		}

		public void onApplicationEvent(ApplicationReadyEvent event) {
			ConfigurableEnvironment env = event.getApplicationContext().getEnvironment();
			Integer pongDelay = env.getProperty("pong.delay", Integer.class, 5000);
			try {
				Thread.sleep(pongDelay);
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
			logger.info("Starting Pong");
			String host = env.getProperty("pong.broker.host", String.class, "localhost");
			Integer port = env.getProperty("pong.broker.port", Integer.class, 8001);
			//MicrometerRSocketInterceptor interceptor = new MicrometerRSocketInterceptor(
			//		meterRegistry, Tag.of("component", "pong"));

			rSocketClient = RoutingRSocketConnector.create()
					.routeId(routeId)
					.serviceName("pong")
					.configure(connector -> {
						/*.addRequesterPlugin(interceptor)*/
						connector.acceptor((setup, sendingSocket) -> Mono
								.just(accept(sendingSocket)));
					})
					.toRSocketClient(TcpClientTransport.create(host, port));

			rSocketClient.source() // proxy
					.block();

			MonoProcessor<Void> onClose = MonoProcessor.create();

			startDaemonThread("pong", onClose);
		}

		@SuppressWarnings("Duplicates")
		RSocket accept(RSocket rSocket) {
			return new RSocketProxy(rSocket) {

				@Override
				public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
					return Flux.from(payloads).map(Payload::getDataUtf8).doOnNext(str -> {
						int received = pingsReceived.incrementAndGet();
						logger.info("received {}({}) in Pong", str, received);
					}).map(PingPongApp::reply).map(reply -> {
						ByteBuf data = ByteBufUtil.writeUtf8(ByteBufAllocator.DEFAULT,
								reply);
						CompositeByteBuf routingMetadata = rSocketClient.allocator().compositeBuffer();
						rSocketClient.encodeAddressMetadata(routingMetadata, "ping");
						return DefaultPayload.create(data, routingMetadata);
					});
				}
			};
		}

	}

}
