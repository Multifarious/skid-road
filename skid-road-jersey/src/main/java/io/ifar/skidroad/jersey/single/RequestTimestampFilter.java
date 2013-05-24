package io.ifar.skidroad.jersey.single;

import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerRequestFilter;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

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
    public ContainerRequest filter(ContainerRequest request) {
        requestTime.set(DateTime.now(dateTimeZone));
        return request;
    }

    public static DateTime getRequestDateTime() {
        return requestTime.get();
    }
}
