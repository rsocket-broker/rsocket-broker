package io.rsocket.routing.http.bridge.core;

import java.util.Arrays;

import io.rsocket.routing.common.Key;
import io.rsocket.routing.common.Tags;

/**
 * @author Olga Maciaszek-Sharma
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
					.forEach(pair -> tagsBuilder
							.with(Key.of(pair.get(0)), pair.get(1)));
		}
		return tagsBuilder.buildTags();
	}
}
