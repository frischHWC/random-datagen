package com.cloudera.frisch.randomdatagen.config;


import lombok.extern.slf4j.Slf4j;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;

import org.springframework.security.web.SecurityFilterChain;


@Slf4j
@Configuration
@EnableWebSecurity
public class SecurityConfig {


    @Bean
    SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
      return http
          .requiresChannel(channel ->
              channel.anyRequest().requiresSecure())
          .authorizeRequests(authorize -> {
              try {
                authorize.anyRequest().permitAll();
              } catch (Exception e) {
                log.error("Could not instantiate authentication, due to error: ", e);
              }
      })
          .build();
    }

}
