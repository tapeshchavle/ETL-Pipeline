package com.food.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.food.io.UserRequest;
import com.food.io.UserResponse;
import com.food.service.UserService;

@RestController
@RequestMapping("/api/user")
//@CrossOrigin(origins = "https://foodingo.netlify.app",allowCredentials = "true")
public class UserController {
	@Autowired
	private UserService userService;
	
	@PostMapping("/register")
	public ResponseEntity<UserResponse> register(@RequestBody UserRequest request) {
		return new ResponseEntity<UserResponse>(userService.registerUser(request),HttpStatus.CREATED);	
	}

}
