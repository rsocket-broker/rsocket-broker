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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * Tests for {@link PathUtils}.
 *
 * @author Olga Maciaszek-Sharma
 * @since 0.3.0
 */
class PathUtilsTests {

	private static final URI URI_WITH_MODE = URI
			.create("http://test.org:8080/rr/address/route");

	private static final URI URI_WITHOUT_MODE = URI
			.create("http://test.org:8080/rr/address/route");

	PathUtilsTests() throws URISyntaxException {
	}

	@Test
	void shouldThrowExceptionWhenTooLittleSegments() throws URISyntaxException {
		URI uri = new URI("http://test.org:8080/fireAndForget");
		assertThatExceptionOfType(IllegalArgumentException.class)
				.isThrownBy(() -> PathUtils.resolveAddress(uri));
	}

	@Test
	void shouldThrowExceptionWhenTooManySegments() throws URISyntaxException {
		URI uri = new URI("http://test.org:8080/fireAndForget/address/route/anotherRoute");
		assertThatExceptionOfType(IllegalArgumentException.class)
				.isThrownBy(() -> PathUtils.resolveAddress(uri));
	}

	@ParameterizedTest
	@ArgumentsSource(UriArgumentsProvider.class)
	void shouldResolveAddress(URI uri) {
		String address = PathUtils.resolveAddress(uri);
		assertThat(address).isEqualTo("address");
	}

	@ParameterizedTest
	@ArgumentsSource(UriArgumentsProvider.class)
	void shouldResolveRoute(URI uri) {
		String address = PathUtils.resolveRoute(uri);
		assertThat(address).isEqualTo("route");
	}

	static class UriArgumentsProvider implements ArgumentsProvider {

		@Override
		public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) throws Exception {
			return Stream.of(URI_WITH_MODE, URI_WITHOUT_MODE).map(Arguments::of);
		}
	}

}