package com.food.repository;

import org.springframework.data.mongodb.repository.MongoRepository;

import com.food.entity.FoodEntity;

public interface FoodRepository extends MongoRepository<FoodEntity,String> {
	

}
