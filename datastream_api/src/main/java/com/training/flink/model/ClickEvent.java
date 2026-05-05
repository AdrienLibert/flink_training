package com.training.flink.model;

public class ClickEvent {
    public String userId;
    public String productId;
    public String category;
    public double price;
    public long timestamp;
    public String action;

    public ClickEvent() {}

    public ClickEvent(String userId, String productId, String category,
                      double price, long timestamp, String action) {
        this.userId = userId;
        this.productId = productId;
        this.category = category;
        this.price = price;
        this.timestamp = timestamp;
        this.action = action;
    }

    @Override
    public String toString() {
        return "ClickEvent{" +
                "userId='" + userId + '\'' +
                ", productId='" + productId + '\'' +
                ", category='" + category + '\'' +
                ", price=" + price +
                ", timestamp=" + timestamp +
                ", action='" + action + '\'' +
                '}';
    }

    public String getAction() {
        return action;
    }

    public String getCategory() {
        return category;
    }

    public double getPrice() {
        return price;
    }

    public String getUserId() {
        return userId;
    }
}
