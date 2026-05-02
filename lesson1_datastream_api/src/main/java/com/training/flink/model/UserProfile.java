package com.training.flink.model;

public class UserProfile {
    public String userId;
    public String tier;

    public UserProfile() {}

    public UserProfile(String userId, String tier) {
        this.userId = userId;
        this.tier = tier;
    }

    public String getUserId() {
        return userId;
    }

    public String getTier() {
        return tier;
    }

    @Override
    public String toString() {
        return "UserProfile{userId='" + userId + "', tier='" + tier + "'}";
    }
}
