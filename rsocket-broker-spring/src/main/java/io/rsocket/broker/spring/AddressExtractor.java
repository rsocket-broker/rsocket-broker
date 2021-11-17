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
import java.util.function.Function;

import io.rsocket.Payload;
import io.rsocket.broker.common.spring.MimeTypes;
import io.rsocket.broker.frames.Address;

import org.springframework.messaging.rsocket.MetadataExtractor;

import static io.rsocket.broker.common.spring.MimeTypes.COMPOSITE_MIME_TYPE;

/**
 * This class uses a Spring MetadataExtractor to extract metadata objects.
 */
public class AddressExtractor implements Function<Payload, Address> {

	private final MetadataExtractor metadataExtractor;

	public AddressExtractor(MetadataExtractor metadataExtractor) {
		this.metadataExtractor = metadataExtractor;
	}

	@Override
	public Address apply(Payload payload) {
		Map<String, Object> payloadMetadata = metadataExtractor
				.extract(payload, COMPOSITE_MIME_TYPE);
		if (payloadMetadata.containsKey(MimeTypes.BROKER_FRAME_METADATA_KEY)) {
			return (Address) payloadMetadata
					.get(MimeTypes.BROKER_FRAME_METADATA_KEY);
		}

		return null;
	}
}
