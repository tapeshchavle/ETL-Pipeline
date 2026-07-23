package com.food.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class CorsConfig implements WebMvcConfigurer {

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/**") // or "/api/**" if you're only using that path
                .allowedOrigins("https://foodingo.netlify.app", "https://foodingo.tapesh.me",
                        "https://admin-foodingo.tapesh.me", "https://app.foodingo.com", "https://localhost", // Add this
                                                                                                             // for
                                                                                                             // Android
                        "http://localhost", "https://foodingo.uttam.me")
                .allowedMethods("GET", "POST", "PUT", "DELETE", "OPTIONS")
                .allowedHeaders("*")
                .allowCredentials(true);
    }
}
