package com.training.flink.model;

import java.io.Serializable;

public class Order implements Serializable {
    public String orderId;
    public String userId;
    public double amount;

    public Order() {}

    public Order(String orderId, String userId, double amount) {
        this.orderId = orderId;
        this.userId = userId;
        this.amount = amount;
    }

    public String toCsv() {
        return orderId + "," + userId + "," + amount;
    }

    @Override
    public String toString() {
        return "Order{" + orderId + ", " + userId + ", " + amount + "}";
    }
}
