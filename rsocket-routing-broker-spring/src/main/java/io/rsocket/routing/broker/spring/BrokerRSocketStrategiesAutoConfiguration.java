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
import io.netty.buffer.Unpooled;
import io.rsocket.routing.frames.Address;
import io.rsocket.routing.frames.AddressFlyweight;
import io.rsocket.routing.frames.BrokerInfo;
import io.rsocket.routing.frames.BrokerInfoFlyweight;
import io.rsocket.routing.frames.RouteSetup;
import io.rsocket.routing.frames.RouteSetupFlyweight;
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

import static io.rsocket.routing.broker.spring.MimeTypes.ADDRESS_MIME_TYPE;
import static io.rsocket.routing.broker.spring.MimeTypes.ROUTE_SETUP_MIME_TYPE;

@Configuration
@AutoConfigureBefore(RSocketStrategiesAutoConfiguration.class)
public class BrokerRSocketStrategiesAutoConfiguration {
	@Bean
	public RSocketStrategiesCustomizer gatewayRSocketStrategiesCustomizer() {
		return strategies -> strategies.decoder(new AddressDecoder(), new BrokerInfoDecoder(), new RouteSetupDecoder())
				.encoder(new AddressEncoder(), new BrokerInfoEncoder(), new RouteSetupEncoder());
	}

	private static abstract class AbstractBrokerEncoder<T> extends AbstractEncoder<T> {

		protected AbstractBrokerEncoder(MimeType... supportedMimeTypes) {
			super(supportedMimeTypes);
		}

		@Override
		public Flux<DataBuffer> encode(Publisher<? extends T> inputStream,
				DataBufferFactory bufferFactory, ResolvableType elementType,
				MimeType mimeType, Map<String, Object> hints) {
			throw new UnsupportedOperationException("stream encoding not supported.");
		}

		@Override
		public DataBuffer encodeValue(T value, DataBufferFactory bufferFactory,
				ResolvableType valueType, MimeType mimeType, Map<String, Object> hints) {
			NettyDataBufferFactory factory = (NettyDataBufferFactory) bufferFactory;
			ByteBuf encoded = encodeValue(factory, value);
			return factory.wrap(encoded);
		}

		protected abstract ByteBuf encodeValue(NettyDataBufferFactory factory, T value);
	}

	private static abstract class AbstractBrokerDecoder<T> extends AbstractDecoder<T> {

		protected AbstractBrokerDecoder(MimeType... supportedMimeTypes) {
			super(supportedMimeTypes);
		}

		@Override
		public Flux<T> decode(Publisher<DataBuffer> inputStream,
				ResolvableType elementType, MimeType mimeType,
				Map<String, Object> hints) {
			throw new UnsupportedOperationException("stream decoding not supported.");
		}

		@Override
		public T decode(DataBuffer buffer, ResolvableType targetType,
				MimeType mimeType, Map<String, Object> hints) throws DecodingException {
			return decode(asByteBuf(buffer));
		}

		protected abstract T decode(ByteBuf byteBuf);
	}

	private static class RouteSetupEncoder extends AbstractBrokerEncoder<RouteSetup> {

		public RouteSetupEncoder() {
			super(ROUTE_SETUP_MIME_TYPE);
		}

		@Override
		protected ByteBuf encodeValue(NettyDataBufferFactory factory, RouteSetup value) {
			return RouteSetupFlyweight
					.encode(factory.getByteBufAllocator(), value.getRouteId(), value
							.getServiceName(), value.getTags());
		}

	}

	private static class RouteSetupDecoder extends AbstractBrokerDecoder<RouteSetup> {

		public RouteSetupDecoder() {
			super(ROUTE_SETUP_MIME_TYPE);
		}

		@Override
		protected RouteSetup decode(ByteBuf byteBuf) {
			return RouteSetup.from(byteBuf);
		}

	}

	private static class AddressEncoder extends AbstractBrokerEncoder<Address> {

		public AddressEncoder() {
			super(ADDRESS_MIME_TYPE);
		}

		@Override
		protected ByteBuf encodeValue(NettyDataBufferFactory factory, Address value) {
			return AddressFlyweight
					.encode(factory.getByteBufAllocator(), value.getOriginRouteId(),
							value.getMetadata(), value.getTags());
		}

	}

	private static class AddressDecoder extends AbstractBrokerDecoder<Address> {

		public AddressDecoder() {
			super(ADDRESS_MIME_TYPE);
		}

		@Override
		protected Address decode(ByteBuf byteBuf) {
			return Address.from(byteBuf);
		}

	}

	private static class BrokerInfoEncoder extends AbstractBrokerEncoder<BrokerInfo> {

		public BrokerInfoEncoder() {
			super(MimeTypes.BROKER_INFO_MIME_TYPE);
		}

		@Override
		protected ByteBuf encodeValue(NettyDataBufferFactory factory, BrokerInfo value) {
			return BrokerInfoFlyweight
					.encode(factory.getByteBufAllocator(), value.getBrokerId(),
							value.getTimestamp(), value.getTags());
		}

	}

	private static class BrokerInfoDecoder extends AbstractBrokerDecoder<BrokerInfo> {

		public BrokerInfoDecoder() {
			super(MimeTypes.BROKER_INFO_MIME_TYPE);
		}

		@Override
		protected BrokerInfo decode(ByteBuf byteBuf) {
			return BrokerInfo.from(byteBuf);
		}

	}

	private static ByteBuf asByteBuf(DataBuffer buffer) {
		return buffer instanceof NettyDataBuffer
				? ((NettyDataBuffer) buffer).getNativeBuffer()
				: Unpooled.wrappedBuffer(buffer.asByteBuffer());
	}
}
