package com.tdw.trino.eventlistener;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tdw.trino.serialized.Payload;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RabbitmqEventListenerTest {
    @Test
    void TestConstructPayload() throws JsonProcessingException {
        List<String> keys = new ArrayList<>();
        keys.add("key1");
        keys.add("key2");
        keys.add("key3");

        Map<String, String> props = new HashMap<>();
        props.put("val1", "foo");
        props.put("val2", "bar");

        Map<String, Integer> v = new HashMap<>();
        v.put("test", 1);

        Payload payload = RabbitmqEventListener.constructPayload(keys, props, v);
        assertEquals(1, payload.getPayload().size());
        Payload layer1 = (Payload) payload.getPayload().get("key1");

        assertEquals(1, layer1.getPayload().size());
        Payload layer2 = (Payload) layer1.getPayload().get("key2");

        assertEquals(3, layer2.getPayload().size());
        assertTrue(layer2.getPayload().containsKey("key3"));
        assertEquals("foo", layer2.getPayload().get("val1"));
        assertEquals("bar", layer2.getPayload().get("val2"));

        Map<String, Integer> innermap = (Map<String, Integer>) layer2.getPayload().get("key3");
        assertEquals(1, innermap.get("test"));

        // Ensure that it serializes as expected
        String json = new ObjectMapper().writeValueAsString(payload);
        assertEquals("{\"key1\":{\"key2\":{\"key3\":{\"test\":1},\"val2\":\"bar\",\"val1\":\"foo\"}}}", json);
    }
}