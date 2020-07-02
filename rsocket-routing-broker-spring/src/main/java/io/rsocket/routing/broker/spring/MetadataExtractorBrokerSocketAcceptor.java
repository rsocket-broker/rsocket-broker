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

import io.rsocket.routing.broker.RSocketIndex;
import io.rsocket.routing.broker.RoutingTable;
import io.rsocket.routing.broker.acceptor.BrokerSocketAcceptor;
import io.rsocket.routing.broker.config.BrokerProperties;
import io.rsocket.routing.broker.rsocket.RoutingRSocketFactory;
import io.rsocket.routing.broker.spring.cluster.ProxyConnections;
import io.rsocket.routing.frames.RoutingFrame;

import org.springframework.messaging.rsocket.MetadataExtractor;
import org.springframework.util.MimeType;

/**
 * Extension of BrokerSocketAcceptor that passes Spring specific functions for reading
 * metadata and consuming BrokerInfo payloads.
 */
public class MetadataExtractorBrokerSocketAcceptor extends BrokerSocketAcceptor {

	public MetadataExtractorBrokerSocketAcceptor(BrokerProperties properties,
			RoutingTable routingTable, RSocketIndex rSocketIndex,
			RoutingRSocketFactory routingRSocketFactory, MetadataExtractor metadataExtractor,
			ProxyConnections proxyConnections) {
		super(properties, routingTable, rSocketIndex, routingRSocketFactory, connectionSetupPayload -> {
			MimeType mimeType = MimeType
					.valueOf(connectionSetupPayload.metadataMimeType());
			Map<String, Object> setupMetadata = metadataExtractor.extract(connectionSetupPayload,
					mimeType);

			if (setupMetadata.containsKey(MimeTypes.ROUTING_FRAME_METADATA_KEY)) {
				return (RoutingFrame) setupMetadata.get(MimeTypes.ROUTING_FRAME_METADATA_KEY);
			}
			return null;
		}, proxyConnections::put, proxyConnections::remove);
	}

}
