package com.food.service;

import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import com.food.repository.AuthenticationFacade;
@Component
public class AuthenticationFacadeImpl implements AuthenticationFacade {
	public Authentication getAuthentication() {
		return SecurityContextHolder.getContext().getAuthentication();
	}

}
