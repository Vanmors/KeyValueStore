package dev.kvstore;

import dev.kvstore.config.BootstrapProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties(BootstrapProperties.class)
public class KVStoreApplication {
    public static void main(String[] args) {
        SpringApplication.run(KVStoreApplication.class, args);
    }
}
