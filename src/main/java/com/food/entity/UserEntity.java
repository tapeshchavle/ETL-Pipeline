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

@Data
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "users")
@Builder
public class UserEntity {
	@Id
	private String id;
	private String name;
	private String email;
	private String password;

	// ── Data Engineering: Audit timestamps ───────────────────────────────────
	@CreatedDate
	private Instant createdAt;   // auto-set when document is first saved

	@LastModifiedDate
	private Instant updatedAt;   // auto-updated on every save

}

