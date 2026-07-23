package com.food.entity;

import java.time.Instant;

import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.mongodb.core.mapping.Document;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "food")
public class FoodEntity {
	
	//@Id if field is String & ObjectId spring will automatically generates the unique id
	// but if field is Long ,Integer ... so we manually generates this
	@Id
	private String id;
	private String name;
	private String description;
	private double price;
	private String category;
	private String imageUrl;

	// ── Data Engineering: Audit timestamps ───────────────────────────────────
	@CreatedDate
	private Instant createdAt;   // auto-set when document is first saved

	@LastModifiedDate
	private Instant updatedAt;   // auto-updated on every save

}

