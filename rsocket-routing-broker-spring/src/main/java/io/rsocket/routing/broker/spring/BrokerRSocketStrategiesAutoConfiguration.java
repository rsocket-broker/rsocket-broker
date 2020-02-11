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
		return strategies -> strategies.decoder(new AddressDecoder(), new RouteSetupDecoder())
				.encoder(new AddressEncoder(), new RouteSetupEncoder());
	}

	private static class RouteSetupEncoder extends AbstractEncoder<RouteSetup> {

		public RouteSetupEncoder() {
			super(ROUTE_SETUP_MIME_TYPE);
		}

		@Override
		public Flux<DataBuffer> encode(Publisher<? extends RouteSetup> inputStream,
				DataBufferFactory bufferFactory, ResolvableType elementType,
				MimeType mimeType, Map<String, Object> hints) {
			throw new UnsupportedOperationException("stream encoding not supported.");
		}

		@Override
		public DataBuffer encodeValue(RouteSetup value, DataBufferFactory bufferFactory,
				ResolvableType valueType, MimeType mimeType, Map<String, Object> hints) {
			NettyDataBufferFactory factory = (NettyDataBufferFactory) bufferFactory;
			ByteBuf encoded = RouteSetupFlyweight
					.encode(factory.getByteBufAllocator(), value.getRouteId(), value
							.getServiceName(), value.getTags());
			return factory.wrap(encoded);
		}

	}

	private static class RouteSetupDecoder extends AbstractDecoder<RouteSetup> {

		public RouteSetupDecoder() {
			super(ROUTE_SETUP_MIME_TYPE);
		}

		@Override
		public Flux<RouteSetup> decode(Publisher<DataBuffer> inputStream,
				ResolvableType elementType, MimeType mimeType,
				Map<String, Object> hints) {
			throw new UnsupportedOperationException("stream decoding not supported.");
		}

		@Override
		public RouteSetup decode(DataBuffer buffer, ResolvableType targetType,
				MimeType mimeType, Map<String, Object> hints) throws DecodingException {
			return RouteSetup.from(asByteBuf(buffer));
		}

	}
	private static class AddressEncoder extends AbstractEncoder<Address> {

		public AddressEncoder() {
			super(ADDRESS_MIME_TYPE);
		}

		@Override
		public Flux<DataBuffer> encode(Publisher<? extends Address> inputStream,
				DataBufferFactory bufferFactory, ResolvableType elementType,
				MimeType mimeType, Map<String, Object> hints) {
			throw new UnsupportedOperationException("stream encoding not supported.");
		}

		@Override
		public DataBuffer encodeValue(Address value, DataBufferFactory bufferFactory,
				ResolvableType valueType, MimeType mimeType, Map<String, Object> hints) {
			NettyDataBufferFactory factory = (NettyDataBufferFactory) bufferFactory;
			ByteBuf encoded = AddressFlyweight
					.encode(factory.getByteBufAllocator(), value.getOriginRouteId(),
							value.getMetadata(), value.getTags());
			return factory.wrap(encoded);
		}

	}

	private static class AddressDecoder extends AbstractDecoder<Address> {

		public AddressDecoder() {
			super(ADDRESS_MIME_TYPE);
		}

		@Override
		public Flux<Address> decode(Publisher<DataBuffer> inputStream,
				ResolvableType elementType, MimeType mimeType,
				Map<String, Object> hints) {
			throw new UnsupportedOperationException("stream decoding not supported.");
		}

		@Override
		public Address decode(DataBuffer buffer, ResolvableType targetType,
				MimeType mimeType, Map<String, Object> hints) throws DecodingException {
			return Address.from(asByteBuf(buffer));
		}

	}

	private static ByteBuf asByteBuf(DataBuffer buffer) {
		return buffer instanceof NettyDataBuffer
				? ((NettyDataBuffer) buffer).getNativeBuffer()
				: Unpooled.wrappedBuffer(buffer.asByteBuffer());
	}
}
