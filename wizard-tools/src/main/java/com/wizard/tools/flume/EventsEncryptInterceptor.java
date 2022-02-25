package com.wizard.tools.flume;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
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
 * In a Hadoop based data warehouse, we usually use Flume to import event logs from Kafka into HDFS
 * and create Hive external tables partitioned by time. One of the keys of this process is to extract
 * the event time from the logs. Flume has a mechanism called Interceptor, It’s possible to write own
 * interceptor, thus do the extraction and conversion in one step.
 */
public class EventsEncryptInterceptor implements Interceptor {
    private static final FastDateFormat dateFormat = FastDateFormat.getInstance("yyyy-MM-dd");
    String encryptFields;
    String tsField;

    EventsEncryptInterceptor(String encryptFields, String tsField) {
        this.encryptFields = encryptFields;
        this.tsField = tsField;
    }

    /**
     * Any initialization / startup needed by the Interceptor.
     */
    @Override
    public void initialize() {
    }

    /**
     * Interception of a single Event.
     *
     * @param event Event to be intercepted
     * @return Modified event that headers contains formatting time
     */
    @Override
    public Event intercept(Event event) {
        // returns a map of name-value pairs describing the data stored in the body.
        Map<String, String> headers = event.getHeaders();
        // constructs a new String by decoding the raw byte array of the data contained in this event using the specified charset.
        String eventBody = new String(event.getBody(), StandardCharsets.UTF_8);
        try {
            // if the given string is null, the Event is to be dropped
            if (Strings.isNullOrEmpty(eventBody)) {
                return null;
            }
            JSONObject jsonObject = JSON.parseObject(eventBody);
            // deserializes json into JSONObject and get time
            Long time = jsonObject.getLong(tsField);
            // formats an object to produce a string: yyyy-MM-dd
            String eventDate = dateFormat.format(time);
            // associates the specified value(yyyy-MM-dd) with the specified key(eventDate) in this map
            headers.put("eventDate", eventDate);
            // get encrypted fields
            String[] toEncryptFields = encryptFields.split(",");
            // encrypt fields
            for (String field : toEncryptFields) {
                String value = jsonObject.getString(field);

            }
            // set the event headers
            event.setHeaders(headers);
        } catch (Exception e) {
            // if parsing fails, associates the specified value(unknow) with the specified key(eventDate) in this map
            headers.put("eventDate", "unknow");
            // // set the event headers
            event.setHeaders(headers);
        }

        return event;
    }

    /**
     * Interception of a batch of events.
     *
     * @param list Input list of events
     * @return Output list of events.
     */
    @Override
    public List<Event> intercept(List<Event> list) {
        return list.stream()
                // returns a stream consisting of the results of applying the intercept function to the elements of this stream.
                .map(this::intercept)
                // returns a stream consisting of the elements of this stream that match the given predicate.
                .filter(Objects::nonNull).collect(Collectors.toList());
    }

    /**
     * Perform any closing / shutdown needed by the Interceptor.
     */
    @Override
    public void close() {
    }

    /**
     * Builder implementations MUST have a no-arg constructor
     */
    public static class TimeBuilder implements Interceptor.Builder {
        String encryptFields;
        String tsField;

        @Override
        public Interceptor build() {
            return new EventsEncryptInterceptor(encryptFields, tsField);
        }

        @Override
        public void configure(Context context) {
            encryptFields = context.getString("encrypt_fields");
            tsField = context.getString("ts_field");
        }
    }
}
