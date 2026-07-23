package com.food.io;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class AuthenticationResponse {
	private String email;
	private String token;

}
