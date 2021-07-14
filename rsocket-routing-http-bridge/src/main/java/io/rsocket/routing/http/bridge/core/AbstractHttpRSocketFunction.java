package io.rsocket.routing.http.bridge.core;

import java.time.Duration;
import java.util.function.Function;

import org.apache.commons.logging.Log;

import static org.apache.commons.logging.LogFactory.getLog;

/**
 * @author Olga Maciaszek-Sharma
 */
abstract class AbstractHttpRSocketFunction<I, O> implements Function<I, O> {

	protected final Log LOG = getLog(getClass());

	// TODO: get from properties; initialise in constructor
	protected final Duration timeout = Duration.ofSeconds(30);

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

}
