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

package io.rsocket.routing.broker.util;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import io.rsocket.routing.common.Id;
import io.rsocket.routing.common.Key;
import io.rsocket.routing.common.Tags;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Object2ObjectHashMap;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;

public class RoaringBitmapIndexedMap<V> implements IndexedMap<Id, V, Tags> {

	private AtomicInteger internalIndex = new AtomicInteger();

	private final Map<Id, Integer> keyToIndex;
	private final Int2ObjectHashMap<V> indexToValue;
	private final Int2ObjectHashMap<Tags> indexToTags;
	private final Table<Key, CharSequence, RoaringBitmap> tagIndexes;

	public RoaringBitmapIndexedMap() {
		this.keyToIndex = Collections.synchronizedMap(new HashMap<>());
		this.indexToValue = new Int2ObjectHashMap<>();
		this.indexToTags = new Int2ObjectHashMap<>();
		this.tagIndexes = new Table<>(new Object2ObjectHashMap<>(), Object2ObjectHashMap::new);
	}

	@Override
	public V get(Id key) {
		Integer index = keyToIndex.get(key);
		if (index != null) {
			return indexToValue.get(index);
		}
		return null;
	}

	@Override
	public V put(Id key, V value, Tags tags) {
		V previousValue;
		int index = keyToIndex
				.computeIfAbsent(key, id -> internalIndex.incrementAndGet());
		synchronized (this) {
			previousValue = indexToValue.put(index, value);
			indexToTags.put(index, tags);
			for (Entry<Key, String> tag : tags.asMap().entrySet()) {
				RoaringBitmap bitmap = tagIndexes.get(tag.getKey(), tag.getValue());
				if (bitmap == null) {
					bitmap = new RoaringBitmap();
					tagIndexes.put(tag.getKey(), tag.getValue(), bitmap);
				}
				bitmap.add(index);
			}
		}
		return previousValue;
	}

	@Override
	public V remove(Id key) {
		Integer index = keyToIndex.remove(key);
		if (index != null) {
			V previousValue = indexToValue.remove(index);
			synchronized (this) {
				Tags tags = indexToTags.remove(index);
				if (tags != null) {
					removeTags(index, tags);
				}
			}
			return previousValue;
		}
		return null;
	}

	private void removeTags(int index, Tags tags) {
		for (Entry<Key, String> tag : tags.asMap().entrySet()) {
			RoaringBitmap bitmap = tagIndexes.get(tag.getKey(), tag.getValue());

			if (bitmap != null) {
				bitmap.remove(index);

				if (bitmap.isEmpty()) {
					tagIndexes.remove(tag.getKey(), tag.getValue());
				}
			}
		}
	}

	@Override
	public int size() {
		return indexToValue.size();
	}

	@Override
	public boolean isEmpty() {
		return indexToValue.isEmpty();
	}

	@Override
	public void clear() {
		synchronized (this) {
			keyToIndex.clear();
			indexToValue.clear();
			indexToTags.clear();
		}
	}

	@Override
	public Collection<V> values() {
		//TODO: any safety issues here?
		return indexToValue.values();
	}

	@Override
	public List<V> query(Tags tags) {
		if (tags == null || tags.asMap().isEmpty()) {
			return new ArrayList<>(indexToValue.values());
		}

		RoaringBitmap result = null;
		for (Entry<Key, String> tag : tags.asMap().entrySet()) {
			RoaringBitmap bitmap = tagIndexes.get(tag.getKey(), tag.getValue());

			if (bitmap == null) {
				return Collections.emptyList();
			}

			if (result == null) {
				// first time thru list
				result = new RoaringBitmap();
				result.or(bitmap);
			} else {
				result.and(bitmap);
			}

			if (result.isEmpty()) {
				// anding empty is always empty
				return Collections.emptyList();
			}

		}

		return new RoaringBitmapList(result);
	}

	private class RoaringBitmapList extends AbstractList<V> {
		private final RoaringBitmap result;

		public RoaringBitmapList(RoaringBitmap result) {
			this.result = result;
		}

		@Override
		public V get(int index) {
			int key = result.select(index);
			return indexToValue.get(key);
		}

		@Override
		public Iterator<V> iterator() {
			return new RoaringBitmapIterator(result.getIntIterator());
		}

		@Override
		public int size() {
			return result.getCardinality();
		}
	}

	private class RoaringBitmapIterator implements Iterator<V> {

		private final IntIterator results;

		public RoaringBitmapIterator(IntIterator results) {
			this.results = results;
		}

		@Override
		public boolean hasNext() {
			return results.hasNext();
		}

		@Override
		public V next() {
			int index = results.next();
			return indexToValue.get(index);
		}
	}
}
