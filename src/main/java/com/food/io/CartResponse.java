package com.food.io;

import java.util.HashMap;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CartResponse {
	private String id;
	private String userId;
	private Map<String, Integer> items=new HashMap<>();
	
}
