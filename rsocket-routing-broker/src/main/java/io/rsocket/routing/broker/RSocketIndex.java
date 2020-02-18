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

package io.rsocket.routing.broker;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import io.rsocket.RSocket;
import io.rsocket.routing.broker.util.IndexedMap;
import io.rsocket.routing.broker.util.RoaringBitmapIndexedMap;
import io.rsocket.routing.common.Id;
import io.rsocket.routing.common.Tags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RSocketIndex implements IndexedMap<Id, RSocket, Tags> {
	private static final Logger logger = LoggerFactory.getLogger(RSocketIndex.class);

	private final IndexedMap<Id, RSocket, Tags> indexedMap = new RoaringBitmapIndexedMap<>();

	private final Function<RSocket, RSocket> rSocketTransformer;

	public RSocketIndex(Function<RSocket, RSocket> rSocketTransformer) {
		this.rSocketTransformer = rSocketTransformer;
	}

	public RSocket get(Id key) {
		return indexedMap.get(key);
	}

	public RSocket put(Id key, RSocket value, Tags indexable) {
		logger.debug("indexing RSocket for Id {} tags {}", key, indexable);
		return indexedMap.put(key, rSocketTransformer.apply(value), indexable);
	}

	public RSocket remove(Id key) {
		return indexedMap.remove(key);
	}

	public int size() {
		return indexedMap.size();
	}

	public boolean isEmpty() {
		return indexedMap.isEmpty();
	}

	public void clear() {
		indexedMap.clear();
	}

	public Collection<RSocket> values() {
		return indexedMap.values();
	}

	public List<RSocket> query(Tags indexable) {
		return indexedMap.query(indexable);
	}

}
