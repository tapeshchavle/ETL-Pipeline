package com.food.service.impl;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.food.entity.CartEntity;
import com.food.event.CartEvent;
import com.food.io.CartRequest;
import com.food.io.CartResponse;
import com.food.repository.CartRepository;
import com.food.service.CartService;
import com.food.service.KafkaPublishingService;
import com.food.service.UserService;

@Service
public class CartServiceImpl implements CartService {

	@Autowired
	private CartRepository cartRepository;

	@Autowired
	private UserService userService;

	// ── Data Engineering: Kafka event publishing ─────────────────────────────
	@Autowired
	private KafkaPublishingService kafkaPublishingService;

	@Override
	public CartResponse addToCart(String foodId) {
		String loggedInUserId = userService.findByUserId();
		Optional<CartEntity> cartOptional = cartRepository.findByUserId(loggedInUserId);
		CartEntity cartEntity = cartOptional.orElseGet(() -> new CartEntity(loggedInUserId, new HashMap<>()));
		Map<String, Integer> cartItems = cartEntity.getItems();
		cartItems.put(foodId, cartItems.getOrDefault(foodId, 0) + 1);
		cartEntity.setItems(cartItems);
		cartEntity = cartRepository.save(cartEntity);

		// ── Publish cart.item_added event ─────────────────────────────────────
		kafkaPublishingService.publish("cart.item_added", loggedInUserId,
				CartEvent.builder()
						.eventType("cart.item_added")
						.userId(loggedInUserId)
						.foodId(foodId)
						.quantity(cartItems.get(foodId))
						.timestamp(Instant.now())
						.build());

		return convertToCartResponse(cartEntity);
	}

	private CartResponse convertToCartResponse(CartEntity entity) {
		return CartResponse.builder()
				.id(entity.getId())
				.items(entity.getItems())
				.userId(entity.getUserId())
				.build();
	}

	@Override
	public CartResponse getCart() {
		String loggedInUserId = userService.findByUserId();
		// Using builder instead of @AllArgsConstructor to avoid breaking change
		// when new fields (createdAt, updatedAt) are added to CartEntity
		CartEntity entity = cartRepository.findByUserId(loggedInUserId)
				.orElse(CartEntity.builder().userId(loggedInUserId).items(new HashMap<>()).build());
		return convertToCartResponse(entity);
	}

	@Override
	public void clearCart() {
		String loggedInUserId = userService.findByUserId();
		cartRepository.deleteByUserId(loggedInUserId);

		// ── Publish cart.cleared event ────────────────────────────────────────
		kafkaPublishingService.publish("cart.cleared", loggedInUserId,
				CartEvent.builder()
						.eventType("cart.cleared")
						.userId(loggedInUserId)
						.timestamp(Instant.now())
						.build());
	}

	@Override
	public CartResponse removeFromCart(CartRequest cartRequest) {
		String loggedInUserId = userService.findByUserId();
		CartEntity entity = cartRepository.findByUserId(loggedInUserId)
				.orElseThrow(() -> new RuntimeException("Cart is not found"));

		Map<String, Integer> cartItems = entity.getItems();
		int newQty = 0;
		if (cartItems.containsKey(cartRequest.getFoodId())) {
			int currentQty = cartItems.get(cartRequest.getFoodId());
			if (currentQty > 1) {
				newQty = currentQty - 1;
				cartItems.put(cartRequest.getFoodId(), newQty);
			} else {
				cartItems.remove(cartRequest.getFoodId());
			}
		}
		entity = cartRepository.save(entity);

		// ── Publish cart.item_removed event ──────────────────────────────────
		kafkaPublishingService.publish("cart.item_removed", loggedInUserId,
				CartEvent.builder()
						.eventType("cart.item_removed")
						.userId(loggedInUserId)
						.foodId(cartRequest.getFoodId())
						.quantity(newQty)
						.timestamp(Instant.now())
						.build());

		return convertToCartResponse(entity);
	}

	@Override
	public String deleteItemFromCart(CartRequest cartRequest) {
		String loggedInUserId = userService.findByUserId();
		CartEntity entity = cartRepository.findByUserId(loggedInUserId)
				.orElseThrow(() -> new RuntimeException("Cart is not found"));

		Map<String, Integer> cartItems = entity.getItems();
		if (cartItems.containsKey(cartRequest.getFoodId())) {
			cartItems.remove(cartRequest.getFoodId());
		}
		cartRepository.save(entity);

		// ── Publish cart.item_deleted event ──────────────────────────────────
		kafkaPublishingService.publish("cart.item_deleted", loggedInUserId,
				CartEvent.builder()
						.eventType("cart.item_deleted")
						.userId(loggedInUserId)
						.foodId(cartRequest.getFoodId())
						.quantity(0)
						.timestamp(Instant.now())
						.build());

		return "Cart is Deleted";
	}

}
