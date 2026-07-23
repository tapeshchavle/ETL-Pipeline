package com.food.repository;

import java.util.Optional;

import org.springframework.data.mongodb.repository.MongoRepository;

import com.food.entity.UserEntity;

public interface UserRepository extends MongoRepository<UserEntity, String> {
	//finder method
	Optional<UserEntity> findByEmail(String email);

}
