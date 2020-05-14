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
import io.rsocket.frame.SetupFrameCodec;
import io.rsocket.routing.broker.config.BrokerProperties;
import io.rsocket.routing.broker.config.BrokerProperties.AbstractAcceptor;
import io.rsocket.routing.broker.config.BrokerProperties.Broker;
import io.rsocket.routing.broker.rsocket.RoutingRSocketFactory;
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

/**
 * Once a broker node has started, this class reaches out to other configured brokers.
 * It makes connections to both the cluster port and the proxy port.
 */
public class ClusterJoinListener implements ApplicationListener<ApplicationReadyEvent> {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	private final BrokerProperties properties;

	private final BrokerConnections brokerConnections;

	private final ProxyConnections proxyConnections;

	private final RSocketMessageHandler messageHandler;

	private final RSocketStrategies strategies;
	private final RoutingRSocketFactory routingRSocketFactory;

	private RSocket rSocket;

	public ClusterJoinListener(BrokerProperties properties, BrokerConnections brokerConnections,
			ProxyConnections proxyConnections, RSocketMessageHandler messageHandler,
			RSocketStrategies strategies, RoutingRSocketFactory routingRSocketFactory) {
		this.properties = properties;
		this.brokerConnections = brokerConnections;
		this.proxyConnections = proxyConnections;
		this.messageHandler = messageHandler;
		this.strategies = strategies;
		this.routingRSocketFactory = routingRSocketFactory;
		setupRSocket();
	}

	@Override
	public void onApplicationEvent(ApplicationReadyEvent event) {
		BrokerInfo localBrokerInfo = BrokerInfo
				.from(properties.getBrokerId()).build();
		// TODO: tags
		for (Broker broker : properties.getBrokers()) {
			logger.info("connecting to {}", broker);

			// TODO: micrometer
			// 1- connect to remote cluster port with the Broker RSocket
			connect(broker.getCluster(), localBrokerInfo, null, rSocket)
					// 2- call remote broker-info to get remote broker-id
					.flatMap(requester -> requester.route("cluster.broker-info")
							.data(localBrokerInfo)
							.retrieveMono(BrokerInfo.class)
							.map(remoteBrokerInfo -> {
								// 3- save cluster requester
								brokerConnections.put(remoteBrokerInfo, requester);
								return remoteBrokerInfo;
							}))
					// 4- connect to remote broker port with a RoutingRSocket
					.flatMap(remoteBrokerInfo -> connect(broker
							.getProxy(), null, localBrokerInfo, routingRSocketFactory.create())
							// 5- save broker requester
							.map(requester2 -> {
								proxyConnections
										.put(remoteBrokerInfo, requester2.rsocket());
								return Tuples.of(remoteBrokerInfo, requester2.rsocket());
							}))
					.subscribe();
		}
	}

	private Mono<RSocketRequester> connect(AbstractAcceptor connection, Object data,
			Object metadata, RSocket rSocket) {
		RSocketRequester.Builder builder = RSocketRequester.builder()
				.rsocketStrategies(strategies)
				.dataMimeType(MimeTypes.ROUTING_FRAME_MIME_TYPE);
		if (data != null) {
			builder.setupData(data);
		}
		if (metadata != null) {
			builder.setupMetadata(metadata, MimeTypes.ROUTING_FRAME_MIME_TYPE);
		}
		return builder.rsocketFactory(rsocketFactory -> rsocketFactory
				.acceptor((setup, sendingSocket) -> Mono.just(rSocket)))
				// TODO: other types?
				.connectTcp(connection.getHost(), connection.getPort());
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
		ByteBuf setup = SetupFrameCodec.encode(byteBufAllocator, false, 1, 1,
				MESSAGE_RSOCKET_COMPOSITE_METADATA.getString(),
				MimeTypes.ROUTING_FRAME_MIME_TYPE.toString(), setupPayload);
		return ConnectionSetupPayload.create(setup);
	}

}
