package com.training.flink.model;

public class Alert {
    public String userId;
    public String reason;

    public Alert() {}

    public Alert(String userId, String reason) {
        this.userId = userId;
        this.reason = reason;
    }

    @Override
    public String toString() {
        return "Alert{userId='" + userId + "', reason='" + reason + "'}";
    }
}
