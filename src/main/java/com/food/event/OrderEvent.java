package com.food.event;

import java.time.Instant;
import java.util.List;

import com.food.io.OrderItem;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Kafka event published to topic "order.created", "payment.verified",
 * "order.status_updated" — captured by the data pipeline for analytics and ML.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderEvent {

    private String eventType;        // order.created | payment.verified | order.status_updated
    private String orderId;
    private String userId;
    private String userAddress;
    private String email;
    private String phoneNumber;
    private List<OrderItem> orderedItems;
    private double amount;
    private String paymentStatus;
    private String orderStatus;
    private String razorpayOrderId;
    private String razorpayPaymentId;
    private Instant timestamp;       // event generation time (UTC)

}
