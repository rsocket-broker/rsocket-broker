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

import java.util.Map;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.rsocket.routing.frames.Address;
import io.rsocket.routing.frames.AddressFlyweight;
import io.rsocket.routing.frames.BrokerInfo;
import io.rsocket.routing.frames.BrokerInfoFlyweight;
import io.rsocket.routing.frames.FrameHeaderFlyweight;
import io.rsocket.routing.frames.FrameType;
import io.rsocket.routing.frames.RouteJoin;
import io.rsocket.routing.frames.RouteJoinFlyweight;
import io.rsocket.routing.frames.RouteRemove;
import io.rsocket.routing.frames.RouteRemoveFlyweight;
import io.rsocket.routing.frames.RouteSetup;
import io.rsocket.routing.frames.RouteSetupFlyweight;
import io.rsocket.routing.frames.RoutingFrame;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.rsocket.RSocketStrategiesAutoConfiguration;
import org.springframework.boot.rsocket.messaging.RSocketStrategiesCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.ResolvableType;
import org.springframework.core.codec.AbstractDecoder;
import org.springframework.core.codec.AbstractEncoder;
import org.springframework.core.codec.DecodingException;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.NettyDataBuffer;
import org.springframework.core.io.buffer.NettyDataBufferFactory;
import org.springframework.util.MimeType;

import static io.rsocket.routing.broker.spring.MimeTypes.ROUTING_FRAME_MIME_TYPE;

@Configuration
@AutoConfigureBefore(RSocketStrategiesAutoConfiguration.class)
public class BrokerRSocketStrategiesAutoConfiguration {
	@Bean
	public RSocketStrategiesCustomizer brokerRSocketStrategiesCustomizer() {
		return strategies -> strategies
				.decoder(new RoutingFrameDecoder())
				.encoder(new RoutingFrameEncoder());
	}

	private static class RoutingFrameEncoder extends AbstractEncoder<RoutingFrame> {

		public RoutingFrameEncoder() {
			super(ROUTING_FRAME_MIME_TYPE);
		}

		@Override
		public Flux<DataBuffer> encode(Publisher<? extends RoutingFrame> inputStream, DataBufferFactory bufferFactory, ResolvableType elementType, MimeType mimeType, Map<String, Object> hints) {
			return Flux.from(inputStream).map(value -> encodeValue(value, bufferFactory,
					elementType, mimeType, hints));		}

		@Override
		public DataBuffer encodeValue(RoutingFrame routingFrame, DataBufferFactory bufferFactory, ResolvableType valueType, MimeType mimeType, Map<String, Object> hints) {
			NettyDataBufferFactory factory = (NettyDataBufferFactory) bufferFactory;

			ByteBufAllocator allocator = factory.getByteBufAllocator();
			switch (routingFrame.getFrameType()) {
			case ADDRESS:
				Address address = (Address) routingFrame;
				return factory.wrap(AddressFlyweight
						.encode(allocator, address.getOriginRouteId(),
								address.getMetadata(), address.getTags()));
			case BROKER_INFO:
				BrokerInfo brokerInfo = (BrokerInfo) routingFrame;
				return factory.wrap(BrokerInfoFlyweight
						.encode(allocator, brokerInfo.getBrokerId(),
								brokerInfo.getTimestamp(), brokerInfo.getTags()));
			case ROUTE_JOIN:
				RouteJoin routeJoin = (RouteJoin) routingFrame;
				return factory
						.wrap(RouteJoinFlyweight.encode(allocator,
								routeJoin.getBrokerId(), routeJoin.getRouteId(), routeJoin
										.getTimestamp(),
								routeJoin.getServiceName(), routeJoin.getTags()));
			case ROUTE_REMOVE:
				RouteRemove routeRemove = (RouteRemove) routingFrame;
				return factory
						.wrap(RouteRemoveFlyweight.encode(allocator,
								routeRemove.getBrokerId(), routeRemove
										.getRouteId(), routeRemove.getTimestamp()));
			case ROUTE_SETUP:
				RouteSetup routeSetup = (RouteSetup) routingFrame;
				return factory
						.wrap(RouteSetupFlyweight.encode(allocator,
								routeSetup.getRouteId(), routeSetup
										.getServiceName(), routeSetup.getTags()));
			}
			throw new IllegalArgumentException("Unknown FrameType " + routingFrame
					.getFrameType());
		}
	}

	private static class RoutingFrameDecoder extends AbstractDecoder<RoutingFrame> {

		public RoutingFrameDecoder() {
			super(ROUTING_FRAME_MIME_TYPE);
		}

		@Override
		public Flux<RoutingFrame> decode(Publisher<DataBuffer> inputStream, ResolvableType elementType, MimeType mimeType, Map<String, Object> hints) {
			return Flux.from(inputStream)
					.map(dataBuffer -> decode(dataBuffer, elementType, mimeType, hints));
		}

		@Override
		public RoutingFrame decode(DataBuffer buffer, ResolvableType targetType, MimeType mimeType, Map<String, Object> hints) throws DecodingException {
			ByteBuf byteBuf = asByteBuf(buffer);
			FrameType frameType = FrameHeaderFlyweight.frameType(byteBuf);
			switch (frameType) {
			case ADDRESS:
				return Address.from(byteBuf);
			case BROKER_INFO:
				return BrokerInfo.from(byteBuf);
			case ROUTE_JOIN:
				return RouteJoin.from(byteBuf);
			case ROUTE_REMOVE:
				return RouteRemove.from(byteBuf);
			case ROUTE_SETUP:
				return RouteSetup.from(byteBuf);
			}
			throw new IllegalArgumentException("Unknown FrameType " + frameType);
		}

	}

	private static ByteBuf asByteBuf(DataBuffer buffer) {
		return buffer instanceof NettyDataBuffer
				? ((NettyDataBuffer) buffer).getNativeBuffer()
				: Unpooled.wrappedBuffer(buffer.asByteBuffer());
	}
}
