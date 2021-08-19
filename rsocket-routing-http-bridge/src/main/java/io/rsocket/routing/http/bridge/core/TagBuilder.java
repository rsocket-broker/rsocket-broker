/*
 * Copyright 2020-2021 the original author or authors.
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

package io.rsocket.routing.http.bridge.core;

import java.util.Arrays;
import java.util.Optional;

import io.rsocket.routing.common.Key;
import io.rsocket.routing.common.Tags;
import io.rsocket.routing.common.WellKnownKey;

/**
 * Utility class for processing routing tags.
 *
 * @author Olga Maciaszek-Sharma
 * @since 0.3.0
 */
final class TagBuilder {

	private TagBuilder() {
		throw new IllegalStateException("Can't instantiate a utility class.");
	}

	static Tags buildTags(String tagString) {
		Tags.Builder<?> tagsBuilder = Tags.builder();
		if (tagString != null) {
			String[] tagPairs = tagString.split(",");
			Arrays.stream(tagPairs)
					.map(pair -> Arrays.asList(pair.split("=")))
					.filter(pair -> pair.size() == 2)
					.forEach(pair -> {
						String key = pair.get(0);
						String value = pair.get(1);
						Optional<String> resolvedKey = Arrays
								.stream(WellKnownKey.values()).map(Enum::name)
								.filter(name -> name.equals(key.toUpperCase()))
								.findAny();
						if (resolvedKey.isPresent()) {
							tagsBuilder
									.with(WellKnownKey.valueOf(resolvedKey.get()), value);
						}
						else {
							tagsBuilder
									.with(Key.of(key), value);
						}

					});
		}
		return tagsBuilder.buildTags();
	}
}
