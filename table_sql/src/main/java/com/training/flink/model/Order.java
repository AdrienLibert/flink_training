package com.training.flink.model;

import java.io.Serializable;
import java.time.Instant;

public class Order implements Serializable {
    public String orderId;
    public String userId;
    public String category;
    public double amount;
    public long ts;

    public Order() {}

    public Order(String orderId, String userId, String category, double amount, long ts) {
        this.orderId = orderId;
        this.userId = userId;
        this.category = category;
        this.amount = amount;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "Order{orderId='" + orderId + "', userId='" + userId + "', category='"
                + category + "', amount=" + amount + ", ts=" + Instant.ofEpochMilli(ts) + '}';
    }
}
