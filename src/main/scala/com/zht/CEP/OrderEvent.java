package com.zht.CEP;
/*
 * @Author root
 * @Data  2022/6/2 11:05
 * @Description
 * */

import lombok.Data;

@Data
public class OrderEvent {
    public String userId;
    public String orderId;
    public String eventType;
    public long timestamp;

    public OrderEvent() {

    }

    public OrderEvent(String userId, String orderId, String eventType, long timestamp) {
        this.userId = userId;
        this.orderId = orderId;
        this.eventType = eventType;
        this.timestamp = timestamp;
    }

}
