package io.rsocket.routing.http.bridge.core;

import io.rsocket.routing.common.Tags;
import org.junit.jupiter.api.Test;

import static io.rsocket.routing.http.bridge.core.TagBuilder.buildTags;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * @author Olga Maciaszek-Sharma
 */
class TagBuilderTests {

	@Test
	void shouldBuildTags() {
		String tagString = "key1=value1,key2=value2";

		Tags tags = buildTags(tagString);

		assertThat(tags.get("key1")).isEqualTo("value1");
		assertThat(tags.get("key2")).isEqualTo("value2");
	}

	@Test
	void shouldIgnoreNonPairTags() {
		String tagString = "key1=value1,key2=value2,valuex,key3=value3";

		Tags tags = buildTags(tagString);

		assertThat(tags.get("key1")).isEqualTo("value1");
		assertThat(tags.get("key2")).isEqualTo("value2");
		assertThat(tags.get("key3")).isEqualTo("value3");
		assertThat(tags.asMap().values()).doesNotContain("valuex");
	}

	@Test
	void shouldNotThrowExceptionOnEmptyTagString() {
		String tagString = "";

		assertThatCode(() -> buildTags(tagString))
				.doesNotThrowAnyException();
	}

	@Test
	void shouldNotThrowExceptionOnNullTagString() {
		String tagString = null;

		assertThatCode(() -> buildTags(tagString))
				.doesNotThrowAnyException();
	}
}
