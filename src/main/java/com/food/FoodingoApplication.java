package com.food;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.mongodb.config.EnableMongoAuditing;

@SpringBootApplication
@EnableMongoAuditing   // Enables @CreatedDate and @LastModifiedDate on all @Document entities
public class FoodingoApplication {

	public static void main(String[] args) {
		SpringApplication.run(FoodingoApplication.class, args);
	}

}
