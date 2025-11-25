package dev.kvstore.config;

import dev.kvstore.sharding.ConsistentHashing;
import dev.kvstore.sharding.ConsistentHashingImpl;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Collections;

//@Configuration
public class ShardingConfig {

//    @Bean
//    public ConsistentHashing consistentHashing() {
//        // При старте кольцо пустое — будет заполнено в BootstrapService
//        return new ConsistentHashingImpl(Collections.emptyList(), 100);
//    }
}