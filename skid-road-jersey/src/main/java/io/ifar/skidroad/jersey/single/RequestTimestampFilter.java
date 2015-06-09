package io.ifar.skidroad.jersey.single;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;

/**
 * Generates a unique UUID for this request and stashes it in a ThreadLocal for the duration of the request.
 */
public class RequestTimestampFilter implements ContainerRequestFilter {
    private static ThreadLocal<DateTime> requestTime = new ThreadLocal<>();

    private final DateTimeZone dateTimeZone;

    public RequestTimestampFilter() {
        this.dateTimeZone = DateTimeZone.UTC;
    }

    public RequestTimestampFilter(DateTimeZone dateTimeZone) {
        this.dateTimeZone = dateTimeZone;
    }

    @Override
    public void filter(ContainerRequestContext requestContext) {
        requestTime.set(DateTime.now(dateTimeZone));
    }

    public static DateTime getRequestDateTime() {
        return requestTime.get();
    }
}
