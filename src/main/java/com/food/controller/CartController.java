package com.food.controller;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.food.io.CartRequest;
import com.food.io.CartResponse;
import com.food.service.CartService;

@RestController
@RequestMapping("/api/cart")
//@CrossOrigin(origins = "https://foodingo.netlify.app",allowCredentials = "true")
public class CartController {
	@Autowired
	private CartService cartService;
	@PostMapping
	public ResponseEntity<?> addToCart(@RequestBody CartRequest request) {
	    String foodId=request.getFoodId();
	    if(foodId==null || foodId.isEmpty()) {
	    	return ResponseEntity.badRequest().body("Food Id is Required");
	    }
	    
	    return new ResponseEntity(cartService.addToCart(foodId),HttpStatus.OK);
	}
	
	@GetMapping
	public CartResponse getCart() {
		return cartService.getCart();
	}
	
	@DeleteMapping("/delete")
	public ResponseEntity deleteById(){
		cartService.clearCart();
		return new ResponseEntity(new String("Cart is Deleted"),HttpStatus.OK);
	}
	
	@PostMapping("/remove")
	public ResponseEntity<?> removeFromCart(@RequestBody CartRequest request){
		String foodId=request.getFoodId();
		
	    if(foodId==null || foodId.isEmpty()) {
	    	return ResponseEntity.badRequest().body("Food Id is Required");
	    }
	  return new ResponseEntity(cartService.removeFromCart(request),HttpStatus.OK);
	}
	@DeleteMapping("/delete-cart")
	public ResponseEntity<?> deleteCart(@RequestBody CartRequest cartRequest){
		return new ResponseEntity<>(cartService.deleteItemFromCart(cartRequest),HttpStatus.OK);		
	}
	
	

}
