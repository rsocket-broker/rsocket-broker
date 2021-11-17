/*
 * Copyright 2021 the original author or authors.
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

package io.rsocket.broker.spring;

import java.util.Map;

import io.rsocket.broker.RSocketIndex;
import io.rsocket.broker.RoutingTable;
import io.rsocket.broker.acceptor.BrokerSocketAcceptor;
import io.rsocket.broker.rsocket.RoutingRSocketFactory;
import io.rsocket.broker.spring.cluster.ProxyConnections;
import io.rsocket.broker.common.spring.MimeTypes;
import io.rsocket.broker.frames.BrokerFrame;

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
		super(properties.getBrokerId(), routingTable, rSocketIndex, routingRSocketFactory, connectionSetupPayload -> {
			MimeType mimeType = MimeType
					.valueOf(connectionSetupPayload.metadataMimeType());
			Map<String, Object> setupMetadata = metadataExtractor.extract(connectionSetupPayload,
					mimeType);

			if (setupMetadata.containsKey(MimeTypes.BROKER_FRAME_METADATA_KEY)) {
				return (BrokerFrame) setupMetadata.get(MimeTypes.BROKER_FRAME_METADATA_KEY);
			}
			return null;
		}, proxyConnections::put, proxyConnections::remove);
	}

}
