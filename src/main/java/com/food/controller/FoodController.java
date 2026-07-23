package com.food.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.food.io.FoodRequest;
import com.food.io.FoodResponse;

import com.food.service.impl.FoodServiceImpl;

@RestController
@RequestMapping("/api/foods")
//@CrossOrigin(origins = "https://foodingo.netlify.app",allowCredentials = "true")
public class FoodController {
	
	@Autowired
	private FoodServiceImpl foodServiceImpl;
	
	@PostMapping("/add")
	public ResponseEntity<FoodResponse> addFood(@RequestPart("food")String foodString,@RequestPart("file") MultipartFile file) {
		ObjectMapper objectMapper=new ObjectMapper();
		FoodRequest request=null;
		try {
			request =objectMapper.readValue(foodString, FoodRequest.class);
		}catch (JsonProcessingException e) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"Invalid Json Format"); 
		}
		return new ResponseEntity<FoodResponse>(foodServiceImpl.addFood(request, file),HttpStatus.CREATED);
	}
	
	@GetMapping
	public ResponseEntity<List<FoodResponse>> readFoods(){
		return new ResponseEntity<>(foodServiceImpl.readFoods(),HttpStatus.OK);
	}
	
	@GetMapping("/{id}")
	public ResponseEntity<FoodResponse> getFoodById(@PathVariable String id) {
		return new ResponseEntity<FoodResponse>(foodServiceImpl.getFoodById(id),HttpStatus.OK);
	}
	@DeleteMapping("/delete/{id}")
	//@ResponseStatus(HttpStatus.NO_CONTENT)
	public ResponseEntity<FoodResponse> deleteFoodById(@PathVariable String  id) {
	   return new ResponseEntity<FoodResponse>(foodServiceImpl.deleteFood(id),HttpStatus.OK);
	}

	
}
