package dev.kvstore.obs;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

@Component
public class TraceFilter extends OncePerRequestFilter {
    @Override
    protected void doFilterInternal(HttpServletRequest req, HttpServletResponse res, FilterChain chain)
            throws ServletException, IOException {
        String rid = Optional.ofNullable(req.getHeader("X-Request-Id"))
                .filter(s -> !s.isBlank())
                .orElse(UUID.randomUUID().toString().substring(0, 12));
        MDC.put("rid", rid);
        try {
            res.setHeader("X-Request-Id", rid);
            chain.doFilter(req, res);
        } finally {
            MDC.clear();
        }
    }
}
