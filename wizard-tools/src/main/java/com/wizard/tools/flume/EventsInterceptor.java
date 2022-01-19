package com.wizard.tools.flume;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Strings;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 *
 */
public class EventsInterceptor implements Interceptor {
    private static final FastDateFormat dateFormat = FastDateFormat.getInstance("yyyy-MM-dd");

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();

        String eventBody = new String(event.getBody(), StandardCharsets.UTF_8);

        try {
            if (Strings.isNullOrEmpty(eventBody)) {
                return null;
            }

            Long time = JSON.parseObject(eventBody).getLong("time");
            String eventDate = dateFormat.format(time);
            headers.put("eventDate", eventDate);
            event.setHeaders(headers);
        } catch (Exception e) {
            headers.put("eventDate", "unknow");
            event.setHeaders(headers);
        }

        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        return list.stream().map(this::intercept)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    @Override
    public void close() {

    }

    public static class TimeBuilder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new EventsInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }


}
