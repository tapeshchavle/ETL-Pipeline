package com.food.io;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
@Builder
@AllArgsConstructor
@Data
@NoArgsConstructor
public class FoodResponse {
	private String id;
	private String name;
	private String description;
	private String imageUrl;
	private double price;
	private String category;
	

}
