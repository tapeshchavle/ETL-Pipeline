package com.food.service;

import com.food.io.CartRequest;
import com.food.io.CartResponse;
import com.food.io.FoodRequest;

public interface CartService {
	
	CartResponse addToCart(String foodId);
	
	CartResponse getCart();
	
	void clearCart();
	
	CartResponse removeFromCart(CartRequest cartRequest);
	
	String deleteItemFromCart(CartRequest request);

}
