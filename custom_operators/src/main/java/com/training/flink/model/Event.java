package com.training.flink.model;

import java.io.Serializable;

public class Event implements Serializable {
    public String key;
    public long value;

    public Event() {}

    public Event(String key, long value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public String toString() {
        return key + "=" + value;
    }
}
