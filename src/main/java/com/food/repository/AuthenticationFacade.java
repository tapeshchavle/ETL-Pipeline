package com.food.repository;

import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

public interface AuthenticationFacade {
	public Authentication getAuthentication();
}
