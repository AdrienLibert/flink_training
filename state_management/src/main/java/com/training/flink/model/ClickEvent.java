package com.training.flink.model;

import java.io.Serializable;

public class ClickEvent implements Serializable {
    public String userId;
    public String category;
    public double price;
    public long timestamp;

    public ClickEvent() {}

    public ClickEvent(String userId, String category, double price, long timestamp) {
        this.userId = userId;
        this.category = category;
        this.price = price;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "ClickEvent{userId='" + userId + "', category='" + category
                + "', price=" + price + ", ts=" + timestamp + '}';
    }
}
