package com.food.controller;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import com.food.io.FoodResponse;
import com.food.service.FoodService;
import com.food.service.UserService;

/**
 * Recommendation endpoint — calls the Python ML service to get personalised
 * food recommendations based on the logged-in user's order history.
 *
 * Fallback: if the ML service is unavailable, returns an empty list (graceful degradation).
 */
@RestController
@RequestMapping("/api/recommendations")
public class RecommendationController {

    private static final Logger log = LoggerFactory.getLogger(RecommendationController.class);

    @Value("${ml.service.url:http://localhost:5001}")
    private String mlServiceUrl;

    @Autowired
    private UserService userService;

    @Autowired
    private FoodService foodService;

    @Autowired
    private RestTemplate restTemplate;

    /**
     * GET /api/recommendations
     * Returns top-5 personalised food recommendations for the logged-in user.
     * Requires JWT authentication.
     */
    @GetMapping
    public ResponseEntity<List<FoodResponse>> getRecommendations() {
        try {
            String userId = userService.findByUserId();

            // Call ML service: GET /recommend/{userId}
            String url = mlServiceUrl + "/recommend/" + userId;
            ResponseEntity<List<String>> mlResponse = restTemplate.exchange(
                    url, HttpMethod.GET, null,
                    new ParameterizedTypeReference<List<String>>() {});

            if (mlResponse.getStatusCode() != HttpStatus.OK || mlResponse.getBody() == null) {
                return ResponseEntity.ok(Collections.emptyList());
            }

            // Fetch FoodResponse for each recommended food ID
            List<FoodResponse> recommendations = mlResponse.getBody().stream()
                    .map(foodId -> {
                        try {
                            return foodService.getFoodById(foodId);
                        } catch (Exception e) {
                            log.warn("Food not found for recommendation id {}: {}", foodId, e.getMessage());
                            return null;
                        }
                    })
                    .filter(food -> food != null)
                    .collect(Collectors.toList());

            return ResponseEntity.ok(recommendations);

        } catch (Exception e) {
            // Graceful degradation — ML service unavailable, return empty list
            log.warn("ML service unavailable for recommendations: {}", e.getMessage());
            return ResponseEntity.ok(Collections.emptyList());
        }
    }

}
