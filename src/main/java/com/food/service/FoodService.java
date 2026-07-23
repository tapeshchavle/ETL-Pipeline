package com.food.service;

import java.util.List;

import org.springframework.web.multipart.MultipartFile;

import com.food.io.FoodRequest;
import com.food.io.FoodResponse;

public interface FoodService {
	//Basically this method takes the food from the admin and save the image in s3 bucket
	String uploadFile(MultipartFile file);
	
	FoodResponse addFood(FoodRequest request,MultipartFile file);
	
	List<FoodResponse> readFoods();
	
	FoodResponse getFoodById(String id);
	
	boolean deleteFile(String file);
	
	FoodResponse deleteFood(String id);

}
