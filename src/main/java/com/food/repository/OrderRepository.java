package com.food.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.data.mongodb.repository.MongoRepository;

import com.food.entity.OrderEntity;

public interface OrderRepository extends MongoRepository<OrderEntity, String> {
   List<OrderEntity> findByUserId(String userId);
   Optional<OrderEntity> findByRazorpayOrderId(String razorpayOrderId);
}
