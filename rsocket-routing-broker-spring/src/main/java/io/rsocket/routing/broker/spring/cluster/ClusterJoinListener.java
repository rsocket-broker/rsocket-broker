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

import java.util.function.Function;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.core.DefaultConnectionSetupPayload;
import io.rsocket.frame.SetupFrameCodec;
import io.rsocket.loadbalance.WeightedStatsRequestInterceptor;
import io.rsocket.plugins.RSocketInterceptor;
import io.rsocket.plugins.RequestInterceptor;
import io.rsocket.routing.broker.locator.WeightedStatsAwareRSocket;
import io.rsocket.routing.broker.rsocket.RoutingRSocketFactory;
import io.rsocket.routing.broker.spring.BrokerProperties;
import io.rsocket.routing.broker.spring.BrokerProperties.Broker;
import io.rsocket.routing.common.spring.ClientTransportFactory;
import io.rsocket.routing.common.spring.MimeTypes;
import io.rsocket.routing.common.spring.TransportProperties;
import io.rsocket.routing.frames.BrokerInfo;
import io.rsocket.transport.ClientTransport;
import io.rsocket.util.DefaultPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import org.springframework.beans.factory.ObjectProvider;
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
	private final ObjectProvider<ClientTransportFactory> transportFactories;

	private RSocket rSocket;

	public ClusterJoinListener(BrokerProperties properties, BrokerConnections brokerConnections,
			ProxyConnections proxyConnections, RSocketMessageHandler messageHandler,
			RSocketStrategies strategies, RoutingRSocketFactory routingRSocketFactory,
			ObjectProvider<ClientTransportFactory> transportFactories) {
		this.properties = properties;
		this.brokerConnections = brokerConnections;
		this.proxyConnections = proxyConnections;
		this.messageHandler = messageHandler;
		this.strategies = strategies;
		this.routingRSocketFactory = routingRSocketFactory;
		this.transportFactories = transportFactories;
		setupRSocket();
	}

	@Override
	public void onApplicationEvent(ApplicationReadyEvent event) {
		BrokerInfo localBrokerInfo = BrokerInfo.from(properties.getBrokerId()).build();
		// TODO: tags
		for (Broker broker : properties.getBrokers()) {
			logger.info("connecting to {}", broker);

			// TODO: micrometer
			// 1- connect to remote cluster port with the Broker RSocket
			RSocketRequester requester = connect(broker.getCluster(), localBrokerInfo, null, rSocket);
			// 2- call remote broker-info to get remote broker-id
			requester.route("cluster.broker-info")
					.data(localBrokerInfo)
					.retrieveMono(BrokerInfo.class)
					.map(remoteBrokerInfo -> {
						// 3- save cluster requester
						brokerConnections.put(remoteBrokerInfo, requester);
						return remoteBrokerInfo;
					})
					// 4- connect to remote broker port with a RoutingRSocket
					.flatMap(remoteBrokerInfo -> {
						RSocketRequester requester2 = connect(broker
								.getProxy(), null, localBrokerInfo, routingRSocketFactory.create());
						return requester2.rsocketClient().source().map(requesterRSocket -> {
							// 5- save broker requester
							proxyConnections.put(remoteBrokerInfo, requesterRSocket);
							return requesterRSocket;
						});
					})
					.subscribe();
		}
	}

	private RSocketRequester connect(TransportProperties transport, Object data,
			Object metadata, RSocket responderRSocket) {
		RSocketRequester.Builder builder = RSocketRequester.builder()
				.rsocketStrategies(strategies)
				.dataMimeType(MimeTypes.ROUTING_FRAME_MIME_TYPE);
		if (data != null) {
			builder.setupData(data);
		}
		if (metadata != null) {
			builder.setupMetadata(metadata, MimeTypes.ROUTING_FRAME_MIME_TYPE);
		}
		builder.rsocketConnector(rSocketConnector -> rSocketConnector

				.interceptors(ir -> ir.forRequester((Function<RSocket, RequestInterceptor>) requesterRSocket -> {
					final WeightedStatsRequestInterceptor weightedStatsRequestInterceptor =
							new WeightedStatsRequestInterceptor();
					ir.forRequester((RSocketInterceptor) rSocket1 -> new WeightedStatsAwareRSocket(rSocket1, weightedStatsRequestInterceptor));
					return weightedStatsRequestInterceptor;
				}))
				.acceptor((setup, sendingSocket) -> Mono.just(responderRSocket)));

		ClientTransport clientTransport = transportFactories.orderedStream()
				.filter(factory -> factory.supports(transport)).findFirst()
				.map(factory -> factory.create(transport))
				.orElseThrow(() -> new IllegalArgumentException("Unknown transport " + properties));

		return builder.transport(clientTransport);
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
		responder.accept(connectionSetupPayload, new RSocket() {
		}).subscribe(rSocket -> this.rSocket = rSocket);
	}

	private ConnectionSetupPayload getConnectionSetupPayload() {
		DataBufferFactory dataBufferFactory = messageHandler.getRSocketStrategies()
				.dataBufferFactory();
		NettyDataBufferFactory ndbf = (NettyDataBufferFactory) dataBufferFactory;
		ByteBufAllocator byteBufAllocator = ndbf.getByteBufAllocator();
		//BrokerInfo brokerInfo = BrokerInfo.from(properties.getBrokerId()).build();
		//ByteBuf encoded = BrokerInfoFlyweight
		//		.encode(byteBufAllocator, brokerInfo.getBrokerId(),
		//				brokerInfo.getTimestamp(), brokerInfo.getTags(), 0);
		Payload setupPayload = DefaultPayload.create(Unpooled.EMPTY_BUFFER, Unpooled.EMPTY_BUFFER);
		//Payload setupPayload = DefaultPayload.create(encoded, Unpooled.EMPTY_BUFFER);
		ByteBuf setup = SetupFrameCodec.encode(byteBufAllocator, false, 1, 1,
				MESSAGE_RSOCKET_COMPOSITE_METADATA.getString(),
				MimeTypes.ROUTING_FRAME_MIME_TYPE.toString(), setupPayload);
		return new DefaultConnectionSetupPayload(setup);
	}

}
