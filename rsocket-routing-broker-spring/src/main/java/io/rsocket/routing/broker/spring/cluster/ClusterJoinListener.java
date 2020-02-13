/*
 * Copyright 2013-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket.routing.broker.spring.cluster;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.rsocket.AbstractRSocket;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.frame.SetupFrameFlyweight;
import io.rsocket.routing.broker.config.BrokerProperties;
import io.rsocket.routing.broker.config.BrokerProperties.Broker;
import io.rsocket.routing.broker.spring.MimeTypes;
import io.rsocket.routing.frames.BrokerInfo;
import io.rsocket.routing.frames.BrokerInfoFlyweight;
import io.rsocket.util.DefaultPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuples;

import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.NettyDataBufferFactory;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.messaging.rsocket.RSocketStrategies;
import org.springframework.messaging.rsocket.annotation.support.RSocketMessageHandler;

import static io.rsocket.metadata.WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA;

public class ClusterJoinListener implements ApplicationListener<ApplicationReadyEvent> {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	private final BrokerProperties properties;

	private final BrokerConnections brokerConnections;

	private final RSocketMessageHandler messageHandler;

	private final RSocketStrategies strategies;

	private RSocket rSocket;

	public ClusterJoinListener(BrokerProperties properties,
			BrokerConnections brokerConnections, RSocketMessageHandler messageHandler, RSocketStrategies strategies) {
		this.properties = properties;
		this.brokerConnections = brokerConnections;
		this.messageHandler = messageHandler;
		this.strategies = strategies;
		setupRSocket();
	}

	@Override
	public void onApplicationEvent(ApplicationReadyEvent event) {
		for (Broker broker : properties.getBrokers()) {
			BrokerInfo brokerInfo = BrokerInfo
					.from(properties.getBrokerId()).build();
			// TODO: tags

			logger.info("connecting to {}", broker);

			// TODO: micrometer
			RSocketRequester.builder().rsocketStrategies(strategies)
					.dataMimeType(MimeTypes.ROUTING_FRAME_MIME_TYPE)
					.setupData(brokerInfo)
					.rsocketFactory(rsocketFactory -> rsocketFactory
							.acceptor(brokerSocketAcceptor()))
					// TODO: other types
					.connectTcp(broker.getHost(), broker.getPort())
					.flatMap(requester -> {
						return requester.route("cluster.broker-info")
								.data(brokerInfo)
								.retrieveMono(BrokerInfo.class)
								.map(bi -> Tuples.of(bi, requester));
					})
					.doOnNext(tuple -> brokerConnections
							.put(tuple.getT1(), tuple.getT2()))
					.subscribe();
		}
	}

	SocketAcceptor brokerSocketAcceptor() {
		return (setup, sendingSocket) -> Mono.just(rSocket);
	}

	/**
	 * For incoming requests to this broker node, the RSocketRequester needs an acceptor
	 * that is able to hand out the RSocket created by MessageHandler. This method
	 * constructs a ConnectionSetupPayload to pass to accept. The resulting RSocket
	 * is then stored in a field for use by the simple socket acceptor above.
	 */
	private void setupRSocket() {
		ConnectionSetupPayload connectionSetupPayload = getConnectionSetupPayload();
		SocketAcceptor responder = this.messageHandler.responder();
		responder.accept(connectionSetupPayload, new AbstractRSocket() {
		}).subscribe(rSocket -> {
			this.rSocket = rSocket;
		});
	}

	private ConnectionSetupPayload getConnectionSetupPayload() {
		DataBufferFactory dataBufferFactory = messageHandler.getRSocketStrategies()
				.dataBufferFactory();
		NettyDataBufferFactory ndbf = (NettyDataBufferFactory) dataBufferFactory;
		ByteBufAllocator byteBufAllocator = ndbf.getByteBufAllocator();
		BrokerInfo brokerInfo = BrokerInfo.from(properties.getBrokerId()).build();
		ByteBuf encodedBrokerInfo = BrokerInfoFlyweight
				.encode(byteBufAllocator, brokerInfo.getBrokerId(),
						brokerInfo.getTimestamp(), brokerInfo.getTags());
		Payload setupPayload = DefaultPayload.create(encodedBrokerInfo,
				Unpooled.EMPTY_BUFFER);
		ByteBuf setup = SetupFrameFlyweight.encode(byteBufAllocator, false, 1, 1,
				MESSAGE_RSOCKET_COMPOSITE_METADATA.getString(),
				MimeTypes.ROUTING_FRAME_MIME_TYPE.toString(), setupPayload);
		return ConnectionSetupPayload.create(setup);
	}

}
