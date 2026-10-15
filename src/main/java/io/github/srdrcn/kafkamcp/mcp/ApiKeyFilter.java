package io.github.srdrcn.kafkamcp.mcp;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.filter.OncePerRequestFilter;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * Tek statik API key: config'teki {@code apiKey} boş değilse istekteki {@code X-Authorization}
 * header'ı birebir eşleşmeli, yoksa 401. {@code apiKey} boşsa filtre hiçbir şey yapmaz.
 */
public class ApiKeyFilter extends OncePerRequestFilter {

	public static final String HEADER = "X-Authorization";

	private static final Logger log = LoggerFactory.getLogger(ApiKeyFilter.class);

	private final byte[] apiKey;

	public ApiKeyFilter(String apiKey) {
		this.apiKey = apiKey == null || apiKey.isBlank() ? null : apiKey.getBytes(StandardCharsets.UTF_8);
	}

	public boolean enabled() {
		return apiKey != null;
	}

	@Override
	protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
			throws ServletException, IOException {
		if (apiKey == null) {
			chain.doFilter(request, response);
			return;
		}
		String provided = request.getHeader(HEADER);
		if (provided != null && MessageDigest.isEqual(provided.getBytes(StandardCharsets.UTF_8), apiKey)) {
			chain.doFilter(request, response);
			return;
		}
		log.atWarn()
			.addKeyValue("remoteAddr", request.getRemoteAddr())
			.addKeyValue("headerPresent", provided != null)
			.log("unauthorized request");
		response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
		response.setContentType("application/json");
		response.setCharacterEncoding("UTF-8");
		response.getWriter()
			.write("{\"error\":\"unauthorized\",\"message\":\"X-Authorization header'ı eksik ya da API key uyuşmuyor\"}");
	}

}
