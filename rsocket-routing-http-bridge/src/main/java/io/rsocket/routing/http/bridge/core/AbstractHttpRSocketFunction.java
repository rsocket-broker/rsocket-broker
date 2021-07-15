package io.rsocket.routing.http.bridge.core;

import java.time.Duration;
import java.util.Arrays;
import java.util.function.Function;

import io.rsocket.routing.client.spring.RoutingRSocketRequester;
import io.rsocket.routing.common.Key;
import io.rsocket.routing.common.Tags;
import io.rsocket.routing.http.bridge.config.RSocketHttpBridgeProperties;
import org.apache.commons.logging.Log;

import static org.apache.commons.logging.LogFactory.getLog;

/**
 * @author Olga Maciaszek-Sharma
 */
abstract class AbstractHttpRSocketFunction<I, O> implements Function<I, O> {

	protected final Log LOG = getLog(getClass());

	protected final RoutingRSocketRequester requester;
	protected final RSocketHttpBridgeProperties properties;
	protected final Duration timeout;

	protected AbstractHttpRSocketFunction(RoutingRSocketRequester requester, RSocketHttpBridgeProperties properties) {
		this.requester = requester;
		this.properties = properties;
		timeout = properties.getTimeout();
	}

	protected void logTimeout(String address, String route) {
		if (LOG.isDebugEnabled()) {
			LOG.debug(String
					.format("Timeout occurred while retrieving RSocket response from address: %s, route: %s. Response was not retrieved within %s", address, route, timeout));
		}
	}

	protected void logException(Throwable error, String address, String route) {
		if (LOG.isErrorEnabled())
			LOG.error(String
					.format("Exception occurred while retrieving RSocket response from address: %s, route: %s", address, route), error);
	}

	protected Tags buildTags(String tagString) {
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
