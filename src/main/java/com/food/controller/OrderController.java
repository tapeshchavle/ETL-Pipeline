package com.food.controller;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.food.io.OrderRequest;
import com.food.io.OrderResponse;
import com.food.service.OrderService;
import com.razorpay.RazorpayException;

@RestController
@RequestMapping("/api/orders")
//@CrossOrigin(origins = "https://foodingo.netlify.app",allowCredentials = "true")
public class OrderController {
	@Autowired
	private OrderService orderService; 
	@PostMapping("/create")
	//@ResponseStatus(HttpStatus.CREATED)
	public ResponseEntity<OrderResponse> createOrderWithPayment(@RequestBody OrderRequest request) throws RazorpayException {
		return new ResponseEntity(orderService.createOrderPayment(request),HttpStatus.CREATED);		
	}
	@PostMapping("/verify")
	@ResponseStatus(HttpStatus.OK)
	public void verifyPayment(@RequestBody Map<String, String > paymentData) {
		orderService.verifyPayment(paymentData,"paid");
		
	}
	@GetMapping
	public List<OrderResponse> getOrders(){
		return orderService.getUserOrders();
	}
	
	@DeleteMapping("/{orderId}")
	@ResponseStatus(HttpStatus.NO_CONTENT)
	public void deleteOrder(@PathVariable String orderId) {
		orderService.removeOrder(orderId);		
	}
	@GetMapping("/{orderId}")	
	public ResponseEntity<OrderResponse> getOrderById(@PathVariable String orderId){
		System.out.println(" Order id is:"+orderId);
		return new ResponseEntity<OrderResponse>(orderService.getOrderById(orderId),HttpStatus.OK);
	}
	
	
	//admin	
	@GetMapping("/all")
	public List<OrderResponse> getOrdersOfAllUsers(){
		return orderService.getAllUserOrdes();
	}
	
	@PatchMapping("/status/{orderId}")
	public void updateOrderStatus(@PathVariable String orderId, @RequestParam String status) {
		orderService.updateOrderStatus(orderId, status);
		
	}
	

}
