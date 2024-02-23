package com.tdw.trino.serialized;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;

import java.util.HashMap;
import java.util.Map;

public class Payload {
    private final Map<String, Object> payload = new HashMap<>();

    @JsonAnyGetter
    public Map<String, Object> any() {
        return payload;
    }

    @JsonAnySetter
    public void set(final String name, final Object value) {
        payload.put(name, value);
    }
}
