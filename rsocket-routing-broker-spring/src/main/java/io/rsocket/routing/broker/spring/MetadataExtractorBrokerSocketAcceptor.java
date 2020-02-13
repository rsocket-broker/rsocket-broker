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

import io.rsocket.metadata.WellKnownMimeType;
import io.rsocket.routing.broker.RSocketIndex;
import io.rsocket.routing.broker.RoutingTable;
import io.rsocket.routing.broker.acceptor.BrokerSocketAcceptor;
import io.rsocket.routing.broker.config.BrokerProperties;
import io.rsocket.routing.broker.locator.RSocketLocator;
import io.rsocket.routing.common.Tags;
import io.rsocket.routing.frames.Address;
import io.rsocket.routing.frames.RouteSetup;

import org.springframework.messaging.rsocket.MetadataExtractor;
import org.springframework.util.MimeType;

public class MetadataExtractorBrokerSocketAcceptor extends BrokerSocketAcceptor {


	public static final MimeType COMPOSITE_MIME_TYPE = MimeType
			.valueOf(WellKnownMimeType.MESSAGE_RSOCKET_COMPOSITE_METADATA.toString());

	public MetadataExtractorBrokerSocketAcceptor(BrokerProperties properties,
			RoutingTable routingTable, RSocketIndex rSocketIndex,
			RSocketLocator rSocketLocator, MetadataExtractor metadataExtractor) {
		super(properties, routingTable, rSocketIndex, rSocketLocator, connectionSetupPayload -> {
			Map<String, Object> setupMetadata = metadataExtractor.extract(connectionSetupPayload,
					MimeType.valueOf(connectionSetupPayload.metadataMimeType()));

			if (setupMetadata.containsKey(MimeTypes.ROUTING_FRAME_METADATA_KEY)) {
				return (RouteSetup) setupMetadata.get(MimeTypes.ROUTING_FRAME_METADATA_KEY);
			}
			return null;
		}, payload -> {
			Map<String, Object> payloadMetadata = metadataExtractor
					.extract(payload, COMPOSITE_MIME_TYPE);
			if (payloadMetadata.containsKey(MimeTypes.ROUTING_FRAME_METADATA_KEY)) {
				Address address = (Address) payloadMetadata.get(MimeTypes.ROUTING_FRAME_METADATA_KEY);
				return address.getTags();
			}

			return Tags.empty();
		});
	}

}
