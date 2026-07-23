package com.food.service;

import com.food.io.UserRequest;
import com.food.io.UserResponse;


public interface UserService {
	
 UserResponse registerUser(UserRequest request);
 
 String findByUserId();

}
