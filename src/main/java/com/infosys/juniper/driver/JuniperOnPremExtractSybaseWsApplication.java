package com.infosys.juniper.driver;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@ComponentScan("com.infosys.juniper.*")
@SpringBootApplication
public class JuniperOnPremExtractSybaseWsApplication {

	public static void main(String[] args) {
		SpringApplication.run(JuniperOnPremExtractSybaseWsApplication.class, args);
	}

}

