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
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.metadata.CompositeMetadataFlyweight;
import io.rsocket.routing.broker.spring.MimeTypes;
import io.rsocket.routing.common.Id;
import io.rsocket.routing.common.Tags;
import io.rsocket.routing.common.WellKnownKey;
import io.rsocket.routing.frames.AddressFlyweight;
import io.rsocket.routing.frames.RouteSetupFlyweight;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.DefaultPayload;
import io.rsocket.util.RSocketProxy;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.messaging.rsocket.RSocketStrategies;

import static io.netty.buffer.Unpooled.EMPTY_BUFFER;
import static io.rsocket.routing.broker.spring.MetadataExtractorBrokerSocketAcceptor.COMPOSITE_MIME_TYPE;

@SpringBootApplication
public class PingPongApp {
	@Bean
	public Ping ping1() {
		return new Ping(1L);
	}

	@Bean
	@ConditionalOnProperty("ping.two.enabled")
	public Ping ping2() {
		return new Ping(2L);
	}

	@Bean
	public Pong pong() {
		return new Pong();
	}

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

	static ByteBuf encodeRouteSetup(RSocketStrategies strategies, Id routeId, String serviceName) {
		Tags tags = Tags.builder()
				.with("current-time", String.valueOf(System.currentTimeMillis()))
				.with(WellKnownKey.TIME_ZONE, System.currentTimeMillis() + "")
				.buildTags();
		ByteBuf routeSetup = RouteSetupFlyweight
				.encode(ByteBufAllocator.DEFAULT, routeId, serviceName, tags);

		CompositeByteBuf composite = ByteBufAllocator.DEFAULT.compositeBuffer();
		CompositeMetadataFlyweight.encodeAndAddMetadata(composite, ByteBufAllocator.DEFAULT,
				MimeTypes.ROUTE_SETUP_MIME_TYPE.toString(), routeSetup);
		return composite;
	}

	static ByteBuf encodeAddress(RSocketStrategies strategies, Id originRouteId, String serviceName) {
		Tags tags = Tags.builder().with(WellKnownKey.SERVICE_NAME, serviceName)
				.buildTags();
		ByteBuf address = AddressFlyweight
				.encode(ByteBufAllocator.DEFAULT, originRouteId, Tags.empty(), tags);

		CompositeByteBuf composite = ByteBufAllocator.DEFAULT.compositeBuffer();
		CompositeMetadataFlyweight.encodeAndAddMetadata(composite, ByteBufAllocator.DEFAULT,
				MimeTypes.ADDRESS_MIME_TYPE.toString(), address);
		return composite;
	}

	public static class Ping
			implements Ordered, ApplicationListener<ApplicationReadyEvent> {

		private Logger logger = LoggerFactory.getLogger(getClass());
		
		@Autowired
		private RSocketStrategies strategies;

		private final Id id;

		private final AtomicInteger pongsReceived = new AtomicInteger();

		private Flux<String> pongFlux;

		public Ping(Long id) {
			this.id = new Id(0, id);
		}

		@Override
		public int getOrder() {
			return 0;
		}

		@Override
		public void onApplicationEvent(ApplicationReadyEvent event) {
			logger.info("Starting Ping {}", id);
			try {
				Thread.sleep(5000);
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
			ConfigurableEnvironment env = event.getApplicationContext().getEnvironment();
			Integer take = env.getProperty("ping.take", Integer.class, null);
			Integer gatewayPort = env.getProperty("spring.rsocket.server.port",
					Integer.class, 8001);

			logger.debug("ping.take: {}", take);

			ByteBuf metadata = encodeRouteSetup(strategies, id, "ping");
			Payload setupPayload = DefaultPayload.create(EMPTY_BUFFER, metadata);

			pongFlux = RSocketFactory.connect().frameDecoder(PayloadDecoder.ZERO_COPY)
					.metadataMimeType(COMPOSITE_MIME_TYPE.toString())
					.setupPayload(setupPayload)//.addRequesterPlugin(interceptor)
					.transport(TcpClientTransport.create(gatewayPort)) // proxy
					.start().log("startPing" + id)
					.flatMapMany(socket -> doPing(take, socket)).cast(String.class)
					.doOnSubscribe(o -> {
						if (logger.isDebugEnabled()) {
							logger.debug("ping doOnSubscribe");
						}
					});

			boolean subscribe = env.getProperty("ping.subscribe", Boolean.class, true);

			if (subscribe) {
				pongFlux.subscribe();
			}
		}

		Publisher<? extends String> doPing(Integer take, RSocket socket) {
			Flux<String> pong = socket
					.requestChannel(Flux.interval(Duration.ofSeconds(1)).map(i -> {
						ByteBuf data = ByteBufUtil.writeUtf8(ByteBufAllocator.DEFAULT,
								"ping" + id);
						ByteBuf routingMetadata = encodeAddress(strategies,
								id, "pong");
						logger.debug("Sending ping" + id);
						return DefaultPayload.create(data, routingMetadata);
						// onBackpressure is needed in case pong is not available yet
					}).log("doPing")
							.onBackpressureDrop(payload -> logger
									.debug("Dropped payload {}", payload.getDataUtf8())))
					.map(Payload::getDataUtf8).doOnNext(str -> {
						int received = pongsReceived.incrementAndGet();
						logger.info("received {}({}) in Ping {}", str, received, id);
					}).doFinally(signal -> socket.dispose());
			if (take != null) {
				return pong.take(take);
			}
			return pong;
		}

		public Flux<String> getPongFlux() {
			return pongFlux;
		}

		public int getPongsReceived() {
			return pongsReceived.get();
		}

	}

	public static class Pong
			implements Ordered, ApplicationListener<ApplicationReadyEvent> {

		private final Id routeId = new Id(0, 3L);
		private Logger logger = LoggerFactory.getLogger(getClass());

		@Autowired
		private RSocketStrategies strategies;

		private final AtomicInteger pingsReceived = new AtomicInteger();

		@Override
		public int getOrder() {
			return -1;
		}

		@Override
		public void onApplicationEvent(ApplicationReadyEvent event) {
			ConfigurableEnvironment env = event.getApplicationContext().getEnvironment();
			Integer pongDelay = env.getProperty("pong.delay", Integer.class, 5000);
			/*try {
				Thread.sleep(pongDelay);
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}*/
			logger.info("Starting Pong");
			Integer gatewayPort = env.getProperty("spring.rsocket.server.port",
					Integer.class, 8001);
			//MicrometerRSocketInterceptor interceptor = new MicrometerRSocketInterceptor(
			//		meterRegistry, Tag.of("component", "pong"));

			ByteBuf metadata = encodeRouteSetup(strategies, routeId, "pong");
			RSocketFactory.connect().metadataMimeType(COMPOSITE_MIME_TYPE.toString())
					.setupPayload(
							DefaultPayload.create(EMPTY_BUFFER, metadata))
					/*.addRequesterPlugin(interceptor)*/.acceptor(this::accept)
					.transport(TcpClientTransport.create(gatewayPort)) // proxy
					.start().block();
		}

		@SuppressWarnings("Duplicates")
		RSocket accept(RSocket rSocket) {
			RSocket pong = new RSocketProxy(rSocket) {

				@Override
				public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
					return Flux.from(payloads).map(Payload::getDataUtf8).doOnNext(str -> {
						int received = pingsReceived.incrementAndGet();
						logger.info("received {}({}) in Pong", str, received);
					}).map(PingPongApp::reply).map(reply -> {
						ByteBuf data = ByteBufUtil.writeUtf8(ByteBufAllocator.DEFAULT,
								reply);
						ByteBuf routingMetadata = encodeAddress(strategies,
								routeId, "ping");
						return DefaultPayload.create(data, routingMetadata);
					});
				}
			};
			return pong;
		}

		public int getPingsReceived() {
			return pingsReceived.get();
		}

	}

}
