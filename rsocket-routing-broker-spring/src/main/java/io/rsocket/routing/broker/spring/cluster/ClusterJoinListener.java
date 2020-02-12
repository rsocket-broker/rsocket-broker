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

import java.math.BigInteger;

import io.rsocket.routing.broker.config.BrokerProperties;
import io.rsocket.routing.broker.config.BrokerProperties.Broker;
import io.rsocket.routing.broker.spring.MimeTypes;
import io.rsocket.routing.frames.BrokerInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.util.function.Tuple2;

import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.messaging.rsocket.RSocketStrategies;

public class ClusterJoinListener implements ApplicationListener<ApplicationReadyEvent> {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	private final BrokerProperties properties;

	private final RSocketStrategies strategies;

	public ClusterJoinListener(BrokerProperties properties,
			RSocketStrategies strategies) {
		this.properties = properties;
		this.strategies = strategies;
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
					//.rsocketFactory(rsocketFactory -> rsocketFactory
					//		.acceptor(brokerSocketAcceptor()))
					// TODO: other types
					.connectTcp(broker.getHost(), broker.getPort())
					// TODO: store requester?
					// TODO: dispose?
					//.map(rSocketRequester -> )
					.subscribe(); //this::registerOutgoing);
		}
	}

	/**
	 * For incoming requests to this broker node, the RSocketRequester needs an acceptor
	 * that is able to hand out GatewayRSocket instances. So here is a very simple one
	 * that just constructs tags metadata and creates a GatewayRSocket.
	 * @return A SocketAcceptor that creates a GatewayRSocket.
	 */
	/*SocketAcceptor brokerSocketAcceptor() {
		return (setup, sendingSocket) -> {
			TagsMetadata.Builder builder = TagsMetadata.builder();
			// TODO: other tags.
			builder.serviceName(properties.getServiceName())
					.routeId(properties.getRouteId().toString());
			return Mono.just(gatewayRSocketFactory.create(builder.build()));
		};
	}*/


	boolean registerOutgoing(Tuple2<BigInteger, RSocketRequester> tuple) {
		//return clusterService.registerOutgoing(tuple.getT1().toString(), tuple.getT2());
		return false;
	}

}
