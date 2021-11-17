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

package io.rsocket.broker.util;

import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

class Table<R, C, V> {
	final Map<R, Map<C, V>> backingMap;
	final Supplier<? extends Map<C, V>> factory;

	Table(Map<R, Map<C, V>> backingMap, Supplier<? extends Map<C, V>> factory) {
		this.backingMap = backingMap;
		this.factory = factory;
	}

	// Accessors

	public boolean contains(Object rowKey, Object columnKey) {
		return rowKey != null && columnKey != null && doContains(rowKey, columnKey);
	}

	public V get(Object rowKey, Object columnKey) {
		return (rowKey == null || columnKey == null) ? null : doGet(rowKey, columnKey);
	}

	public boolean isEmpty() {
		return backingMap.isEmpty();
	}

	public int size() {
		int size = 0;
		for (Map<C, V> map : backingMap.values()) {
			size += map.size();
		}
		return size;
	}

	// Mutators

	public void clear() {
		backingMap.clear();
	}

	private Map<C, V> getOrCreate(R rowKey) {
		Map<C, V> map = backingMap.get(rowKey);
		if (map == null) {
			map = factory.get();
			backingMap.put(rowKey, map);
		}
		return map;
	}

	public V put(R rowKey, C columnKey, V value) {
		Objects.requireNonNull(rowKey);
		Objects.requireNonNull(columnKey);
		Objects.requireNonNull(value);
		return getOrCreate(rowKey).put(columnKey, value);
	}

	public V remove(Object rowKey, Object columnKey) {
		if ((rowKey == null) || (columnKey == null)) {
			return null;
		}
		Map<C, V> map = safeGet(backingMap, rowKey);
		if (map == null) {
			return null;
		}
		V value = map.remove(columnKey);
		if (map.isEmpty()) {
			backingMap.remove(rowKey);
		}
		return value;
	}

	public Map<C, V> row(R rowKey) {
		return backingMap.get(rowKey);
	}

	// Views

	public Map<R, Map<C, V>> rowMap() {
		return backingMap;
	}

	protected boolean doContains(Object rowKey, Object columnKey) {
		Map<C, V> row = safeGet(rowMap(), rowKey);
		return row != null && safeContainsKey(row, columnKey);
	}

	protected V doGet(Object rowKey, Object columnKey) {
		Map<C, V> row = safeGet(rowMap(), rowKey);
		return (row == null) ? null : safeGet(row, columnKey);
	}

	/** Returns the string representation {@code rowMap().toString()}. */
	public String toString() {
		return rowMap().toString();
	}

	static boolean safeContainsKey(Map<?, ?> map, Object key) {
		Objects.requireNonNull(map);
		try {
			return map.containsKey(key);
		} catch (ClassCastException | NullPointerException e) {
			return false;
		}
	}

	static <V> V safeGet(Map<?, V> map, Object key) {
		Objects.requireNonNull(map);
		try {
			return map.get(key);
		} catch (ClassCastException | NullPointerException e) {
			return null;
		}
	}

	static <V> V safeRemove(Map<?, V> map, Object key) {
		Objects.requireNonNull(map);
		try {
			return map.remove(key);
		} catch (ClassCastException | NullPointerException e) {
			return null;
		}
	}
}
