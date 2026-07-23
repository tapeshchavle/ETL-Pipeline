package com.food.service;

import java.util.List;
import java.util.Map;

import com.food.io.OrderRequest;
import com.food.io.OrderResponse;
import com.razorpay.RazorpayException;

public interface OrderService {
	OrderResponse createOrderPayment(OrderRequest request) throws RazorpayException;
    
	void verifyPayment(Map<String, String> paymentData,String status);
	
	//orders of loggedin user
	List<OrderResponse> getUserOrders();
	
	void removeOrder(String orderId);
	
	//for admin side
	List<OrderResponse> getAllUserOrdes();
	
	void updateOrderStatus(String orderId,String status);
	
	OrderResponse getOrderById(String id);	
	
	
	
	
	
}
