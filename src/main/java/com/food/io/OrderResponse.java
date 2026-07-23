package com.food.io;

import java.util.List;

import lombok.Builder;
import lombok.Data;
@Data
@Builder
public class OrderResponse {
	private String id;
	private String userId;
	private String userAddress;
	private String phoneNumber;
	private String email;
	private List<OrderItem> orderedItems;
	private double amount;
	private String paymentStatus;
	private String razorpayOrderId;
	private String orderStatus;

}
