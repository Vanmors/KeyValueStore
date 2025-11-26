package dev.kvstore.obs;

import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.boot.web.client.RestTemplateCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class HttpClientTracingConfig {
    @Bean
    RestTemplateCustomizer restTemplateCustomizer() {
        return rt -> rt.getInterceptors().add((req, body, exec) -> {
            String rid = MDC.get("rid");
            if (rid != null) req.getHeaders().add("X-Request-Id", rid);
            long t0 = System.nanoTime();
            try {
                var rsp = exec.execute(req, body);
                long ms = (System.nanoTime() - t0) / 1_000_000;
                LoggerFactory.getLogger("HTTP").info("[HTTP] {} {} -> {} in {} ms",
                        req.getMethod(), req.getURI(), rsp.getStatusCode().value(), ms);
                return rsp;
            } catch (Exception e) {
                long ms = (System.nanoTime() - t0) / 1_000_000;
                LoggerFactory.getLogger("HTTP").warn("[HTTP] {} {} FAILED in {} ms err={}",
                        req.getMethod(), req.getURI(), ms, e.toString());
                throw e;
            }
        });
    }
}
