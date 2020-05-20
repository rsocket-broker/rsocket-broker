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

import java.util.Collection;
import java.util.List;

public interface IndexedMap<K, V, IDX> {

	/**
	 * Returns the value to which the specified key is mapped,
	 * or {@code null} if this map contains no mapping for the key.
	 * @param key the key whose associated value is to be returned
	 * @return the value to which the specified key is mapped, or
	 *      {@code null} if this map contains no mapping for the key
	 */
	V get(K key);

	/**
	 * Associates the specified value and indexable with the specified key in this map.
	 * @param key key with which the specified value is to be associated
	 * @param value value to be associated with the specified key
	 * @param indexable indexable to be associated with the specified key
	 * @return the previous value associated with <tt>key</tt>, or
	 *      <tt>null</tt> if there was no mapping for <tt>key</tt>.
	 */
	V put(K key, V value, IDX indexable);

	/**
	 * Removes the mapping for a key from this map if it is present.
	 * @param key key whose mapping is to be removed from the map
	 * @return the previous value associated with <tt>key</tt>, or
	 *      <tt>null</tt> if there was no mapping for <tt>key</tt>.
	 */
	V remove(K key);

	/**
	 * Returns the number of key-value mappings in this map.
	 * @return the number of key-value mappings in this map
	 */
	int size();

	/**
	 * Returns <tt>true</tt> if this map contains no key-value mappings.
	 * @return <tt>true</tt> if this map contains no key-value mappings
	 */
	boolean isEmpty();

	/**
	 * Removes all of the mappings from this map.
	 */
	void clear();

	/**
	 * Returns a {@link Collection} view of the values contained in this map.
	 * @return a collection view of the values contained in this map
	 */
	Collection<V> values();

	/**
	 * Returns a {@link List} view of the values matching the indexable in this map.
	 * @param indexable the indexable query parameter
	 * @return a list view of the values matching the indexable in this map.
	 */
	List<V> query(IDX indexable);
}
