package com.food.event;

import java.time.Instant;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Kafka event published to topics: "user.registered", "user.login"
 * — used for user funnel analytics and churn prediction.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UserEvent {

    private String eventType;   // user.registered | user.login
    private String userId;
    private String email;
    private String name;        // populated only for user.registered
    private Instant timestamp;  // event generation time (UTC)

}
