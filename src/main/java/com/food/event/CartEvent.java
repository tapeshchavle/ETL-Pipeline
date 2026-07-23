package com.food.event;

import java.time.Instant;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Kafka event published to topics: "cart.item_added", "cart.item_removed",
 * "cart.cleared", "cart.item_deleted" — used for cart abandonment analysis.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CartEvent {

    private String eventType;   // cart.item_added | cart.item_removed | cart.cleared | cart.item_deleted
    private String userId;
    private String foodId;      // null when eventType = cart.cleared
    private int quantity;       // quantity after operation (0 if removed)
    private Instant timestamp;  // event generation time (UTC)

}
