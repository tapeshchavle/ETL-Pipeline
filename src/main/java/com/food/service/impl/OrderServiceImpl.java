package com.food.service.impl;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.food.entity.OrderEntity;
import com.food.event.OrderEvent;
import com.food.io.OrderRequest;
import com.food.io.OrderResponse;
import com.food.repository.CartRepository;
import com.food.repository.OrderRepository;
import com.food.service.KafkaPublishingService;
import com.food.service.OrderService;
import com.food.service.UserService;
import com.razorpay.Order;
import com.razorpay.RazorpayClient;
import com.razorpay.RazorpayException;

@Service
public class OrderServiceImpl implements OrderService {

	@Autowired
	private OrderRepository orderRepository;

	@Autowired
	private CartRepository cartRepository;

	@Value("${razorpay.key}")
	private String RAZORPAY_KEY;

	@Value("${razorpay.secret}")
	private String RAZORPAY_SECRET;

	@Autowired
	private UserService userService;

	// ── Data Engineering: Kafka event publishing ─────────────────────────────
	@Autowired
	private KafkaPublishingService kafkaPublishingService;

	@Override
	public OrderResponse createOrderPayment(OrderRequest request) throws RazorpayException {
		OrderEntity orderEntity = convertToOrderEntity(request);
		orderEntity = orderRepository.save(orderEntity);

		// Razorpay order creation
		RazorpayClient razorpayClient = new RazorpayClient(RAZORPAY_KEY, RAZORPAY_SECRET);
		JSONObject orderRequest = new JSONObject();
		orderRequest.put("amount", orderEntity.getAmount());
		orderRequest.put("currency", "INR");
		orderRequest.put("payment_capture", 1);
		Order razorpayOrder = razorpayClient.orders.create(orderRequest);
		orderEntity.setRazorpayOrderId(razorpayOrder.get("id"));
		String loggedInUserId = userService.findByUserId();
		orderEntity.setUserId(loggedInUserId);
		orderEntity.setOrderStatus(request.getOrderStatus());
		orderEntity = orderRepository.save(orderEntity);

		// ── Publish order.created event to data pipeline ─────────────────────
		kafkaPublishingService.publish("order.created", orderEntity.getId(),
				OrderEvent.builder()
						.eventType("order.created")
						.orderId(orderEntity.getId())
						.userId(orderEntity.getUserId())
						.userAddress(orderEntity.getUserAddress())
						.email(orderEntity.getEmail())
						.phoneNumber(orderEntity.getPhoneNumber())
						.orderedItems(orderEntity.getOrderedItems())
						.amount(orderEntity.getAmount())
						.paymentStatus(orderEntity.getPaymentStatus())
						.orderStatus(orderEntity.getOrderStatus())
						.razorpayOrderId(orderEntity.getRazorpayOrderId())
						.timestamp(Instant.now())
						.build());

		return convertToOrderResponse(orderEntity);
	}

	private OrderEntity convertToOrderEntity(OrderRequest request) {
		return OrderEntity.builder()
				.userAddress(request.getUserAddress())
				.amount(request.getAmount())
				.orderedItems(request.getOrderedItems())
				.email(request.getEmail())
				.phoneNumber(request.getPhoneNumber())
				.paymentStatus(request.getOrderStatus())
				.build();
	}

	private OrderResponse convertToOrderResponse(OrderEntity orderEntity) {
		return OrderResponse.builder()
				.id(orderEntity.getId())
				.userId(orderEntity.getUserId())
				.userAddress(orderEntity.getUserAddress())
				.phoneNumber(orderEntity.getPhoneNumber())
				.email(orderEntity.getEmail())
				.orderedItems(orderEntity.getOrderedItems())
				.amount(orderEntity.getAmount())
				.paymentStatus(orderEntity.getPaymentStatus())
				.orderStatus(orderEntity.getOrderStatus())
				.razorpayOrderId(orderEntity.getRazorpayOrderId())
				.build();
	}

	@Override
	public void verifyPayment(Map<String, String> paymentData, String status) {
		String razorpayOrderId = paymentData.get("razorpay_order_id");
		OrderEntity existingOrder = orderRepository.findByRazorpayOrderId(razorpayOrderId)
				.orElseThrow(() -> new RuntimeException("Order not found"));

		existingOrder.setPaymentStatus(status);
		existingOrder.setRazorpaySignature(paymentData.get("razorpay_signature"));
		existingOrder.setRazorpayPaymentId(paymentData.get("razorpay_payment_id"));
		orderRepository.save(existingOrder);

		if ("paid".equalsIgnoreCase(status)) {
			cartRepository.deleteByUserId(existingOrder.getUserId());
		}

		// ── Publish payment.verified event to data pipeline ───────────────────
		kafkaPublishingService.publish("payment.verified", existingOrder.getId(),
				OrderEvent.builder()
						.eventType("payment.verified")
						.orderId(existingOrder.getId())
						.userId(existingOrder.getUserId())
						.amount(existingOrder.getAmount())
						.paymentStatus(status)
						.razorpayOrderId(razorpayOrderId)
						.razorpayPaymentId(paymentData.get("razorpay_payment_id"))
						.timestamp(Instant.now())
						.build());
	}

	@Override
	public List<OrderResponse> getUserOrders() {
		String loggedInUserId = userService.findByUserId();
		List<OrderEntity> orderEntities = orderRepository.findByUserId(loggedInUserId);
		return orderEntities.stream().map(e -> convertToOrderResponse(e)).collect(Collectors.toList());
	}

	@Override
	public void removeOrder(String orderId) {
		orderRepository.deleteById(orderId);
	}

	@Override
	public List<OrderResponse> getAllUserOrdes() {
		List<OrderEntity> orderEntities = orderRepository.findAll();
		return orderEntities.stream().map(entity -> convertToOrderResponse(entity)).collect(Collectors.toList());
	}

	@Override
	public void updateOrderStatus(String orderId, String status) {
		OrderEntity entity = orderRepository.findById(orderId)
				.orElseThrow(() -> new RuntimeException("order not found"));
		entity.setOrderStatus(status);
		orderRepository.save(entity);

		// ── Publish order.status_updated event to data pipeline ───────────────
		kafkaPublishingService.publish("order.status_updated", orderId,
				OrderEvent.builder()
						.eventType("order.status_updated")
						.orderId(orderId)
						.userId(entity.getUserId())
						.orderStatus(status)
						.timestamp(Instant.now())
						.build());
	}

	@Override
	public OrderResponse getOrderById(String id) {
		OrderEntity order = orderRepository.findById(id).orElse(null);
		if (order != null) {
			return convertToOrderResponse(order);
		}
		return null;
	}
}
