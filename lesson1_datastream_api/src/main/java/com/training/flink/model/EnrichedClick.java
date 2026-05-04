package com.training.flink.model;

public class EnrichedClick {
    public ClickEvent click;
    public String tier;

    public EnrichedClick() {}

    public EnrichedClick(ClickEvent click, String tier) {
        this.click = click;
        this.tier = tier;
    }

    @Override
    public String toString() {
        return "EnrichedClick{tier='" + tier + "', userId='" + click.userId
                + "', action='" + click.action + "', product='" + click.productId + "'}";
    }
}
