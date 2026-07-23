package com.food.controller;

import java.time.Instant;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.food.event.UserEvent;
import com.food.io.AuthenticationRequest;
import com.food.io.AuthenticationResponse;
import com.food.repository.UserRepository;
import com.food.service.AppUserDetailsService;
import com.food.service.KafkaPublishingService;
import com.food.util.JwtUtil;

import lombok.AllArgsConstructor;

@RestController
@RequestMapping("/api/user")
@AllArgsConstructor
//@CrossOrigin(origins = "https://foodingo.netlify.app",allowCredentials = "true")
public class AuthController {

	private final AuthenticationManager authenticationManager;
	private final AppUserDetailsService userDetailsService;
	private final JwtUtil jwtUtil;
	private final UserRepository userRepository;                 // to fetch userId for event
	private final KafkaPublishingService kafkaPublishingService; // data pipeline

	@PostMapping("/login")
	public AuthenticationResponse login(@RequestBody AuthenticationRequest request) {
		authenticationManager.authenticate(
				new UsernamePasswordAuthenticationToken(request.getEmail(), request.getPassword()));

		UserDetails userDetails = userDetailsService.loadUserByUsername(request.getEmail());
		final String jwtToken = jwtUtil.generateToken(userDetails);

		// ── Publish user.login event to data pipeline (async, fire-and-forget) ──
		String userId = userRepository.findByEmail(request.getEmail())
				.map(u -> u.getId()).orElse("unknown");

		kafkaPublishingService.publish("user.login", userId,
				UserEvent.builder()
						.eventType("user.login")
						.userId(userId)
						.email(request.getEmail())
						.timestamp(Instant.now())
						.build());

		return new AuthenticationResponse(request.getEmail(), jwtToken);
	}

}
