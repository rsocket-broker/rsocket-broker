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
import java.util.function.Function;

import io.rsocket.Payload;
import io.rsocket.routing.common.Tags;
import io.rsocket.routing.frames.Address;

import org.springframework.messaging.rsocket.MetadataExtractor;

import static io.rsocket.routing.broker.spring.MimeTypes.COMPOSITE_MIME_TYPE;

/**
 * This class uses a Spring MetadataExtractor to extract metadata objects.
 */
public class AddressTagsExtractor implements Function<Payload, Tags> {

	private final MetadataExtractor metadataExtractor;

	public AddressTagsExtractor(MetadataExtractor metadataExtractor) {
		this.metadataExtractor = metadataExtractor;
	}

	@Override
	public Tags apply(Payload payload) {
		Map<String, Object> payloadMetadata = metadataExtractor
				.extract(payload, COMPOSITE_MIME_TYPE);
		if (payloadMetadata.containsKey(MimeTypes.ROUTING_FRAME_METADATA_KEY)) {
			Address address = (Address) payloadMetadata
					.get(MimeTypes.ROUTING_FRAME_METADATA_KEY);
			return address.getTags();
		}

		return Tags.empty();
	}
}
