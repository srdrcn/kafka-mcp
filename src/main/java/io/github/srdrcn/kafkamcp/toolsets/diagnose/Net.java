package io.github.srdrcn.kafkamcp.toolsets.diagnose;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NoRouteToHostException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.github.srdrcn.kafkamcp.mcp.Results;

/**
 * Süre sınırlı ağ kontrolleri: host:port listesi ayrıştırma, DNS çözümleme ve TCP bağlantı denemesi; hatalar
 * Türkçe açıklanır (DNS, connection refused, zaman aşımı → firewall/NetworkPolicy/egress, ağ yolu yok).
 */
public final class Net {

	/** Hata tipleri. */
	public static final String DNS = "dns";

	public static final String DNS_TIMEOUT = "dnsTimeout";

	public static final String REFUSED = "refused";

	public static final String TIMEOUT = "timeout";

	public static final String NO_ROUTE = "noRoute";

	public static final String ERROR = "error";

	private static final int MAX_ADDRESSES = 5;

	private Net() {
	}

	/** host:port. */
	public record Endpoint(String host, int port) {

		public String address() {
			return host.contains(":") ? "[" + host + "]:" + port : host + ":" + port;
		}

	}

	/** Ayrıştırılmış adres listesi ve geçersiz girdiler. */
	public record Parsed(List<Endpoint> endpoints, List<String> invalid) {
	}

	/** "h1:9092,SASL_SSL://h2:9093,[::1]:9092" → endpoint'ler (protokol öneki atlanır). */
	public static Parsed parseServers(String servers) {
		List<Endpoint> out = new ArrayList<>();
		List<String> invalid = new ArrayList<>();
		if (servers == null) {
			return new Parsed(out, invalid);
		}
		for (String raw : servers.split(",")) {
			String s = raw.trim();
			if (s.isEmpty()) {
				continue;
			}
			int scheme = s.indexOf("://");
			String v = scheme >= 0 ? s.substring(scheme + 3) : s;
			String host;
			String port;
			if (v.startsWith("[")) {
				int close = v.indexOf(']');
				if (close < 0 || close + 1 >= v.length() || v.charAt(close + 1) != ':') {
					invalid.add(s);
					continue;
				}
				host = v.substring(1, close);
				port = v.substring(close + 2);
			}
			else {
				int colon = v.lastIndexOf(':');
				if (colon <= 0 || v.indexOf(':') != colon) {
					invalid.add(s);
					continue;
				}
				host = v.substring(0, colon);
				port = v.substring(colon + 1);
			}
			try {
				int p = Integer.parseInt(port.trim());
				if (p < 1 || p > 65535 || host.isBlank()) {
					throw new NumberFormatException();
				}
				out.add(new Endpoint(host.trim(), p));
			}
			catch (NumberFormatException e) {
				invalid.add(s);
			}
		}
		return new Parsed(out, invalid);
	}

	/** DNS sonucu: adresler (en fazla 5) ya da hata tipi ve Türkçe açıklaması. */
	public record Resolution(String host, List<InetAddress> addresses, long durationMs, String errorType,
			String error) {

		public boolean ok() {
			return errorType == null;
		}

		public Map<String, Object> toMap() {
			return Results.map("host", host, "status", ok() ? "ok" : "error", "durationMs", durationMs, "addresses",
					ok() ? addresses.stream().map(InetAddress::getHostAddress).toList() : null, "error", error);
		}

	}

	/** Host'u en fazla timeoutMs içinde çözer. */
	public static Resolution resolve(String host, long timeoutMs) {
		long start = System.nanoTime();
		CompletableFuture<InetAddress[]> f = new CompletableFuture<>();
		Thread.ofVirtual().name("diagnose-dns").start(() -> {
			try {
				f.complete(InetAddress.getAllByName(host));
			}
			catch (Throwable t) {
				f.completeExceptionally(t);
			}
		});
		try {
			InetAddress[] all = f.get(Math.max(1, timeoutMs), TimeUnit.MILLISECONDS);
			List<InetAddress> list = Arrays.asList(all).subList(0, Math.min(MAX_ADDRESSES, all.length));
			return new Resolution(host, List.copyOf(list), elapsed(start), null, null);
		}
		catch (TimeoutException e) {
			return new Resolution(host, List.of(), elapsed(start), DNS_TIMEOUT, explain(DNS_TIMEOUT, host, timeoutMs, null));
		}
		catch (ExecutionException e) {
			Throwable c = e.getCause();
			String type = c instanceof UnknownHostException ? DNS : ERROR;
			return new Resolution(host, List.of(), elapsed(start), type, explain(type, host, timeoutMs, c));
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return new Resolution(host, List.of(), elapsed(start), ERROR, "DNS sorgusu iptal edildi");
		}
	}

	/** TCP bağlantı denemesi sonucu. */
	public record Probe(String target, String address, long durationMs, String errorType, String error) {

		public boolean ok() {
			return errorType == null;
		}

		public Map<String, Object> toMap() {
			return Results.map("target", target, "address", address, "status", ok() ? "ok" : "error", "durationMs",
					durationMs, "error", error);
		}

	}

	/** Endpoint'e (çözülmüş adresine) en fazla timeoutMs içinde TCP bağlantısı kurup kapatır. */
	public static Probe connect(Endpoint e, InetAddress address, long timeoutMs) {
		long start = System.nanoTime();
		String target = e.address();
		String addr = address == null ? null : address.getHostAddress();
		try (Socket s = new Socket()) {
			InetSocketAddress sa = address == null ? new InetSocketAddress(e.host(), e.port())
					: new InetSocketAddress(address, e.port());
			if (sa.isUnresolved()) {
				return new Probe(target, null, elapsed(start), DNS, explain(DNS, e.host(), timeoutMs, null));
			}
			s.connect(sa, (int) Math.max(1, Math.min(Integer.MAX_VALUE, timeoutMs)));
			return new Probe(target, addr != null ? addr : sa.getAddress().getHostAddress(), elapsed(start), null, null);
		}
		catch (IOException ex) {
			String type = errorType(ex);
			return new Probe(target, addr, elapsed(start), type, explain(type, target, timeoutMs, ex));
		}
	}

	/** Endpoint'i çözüp bağlanır (DNS + TCP; toplam en fazla dnsTimeout + connectTimeout). */
	public static Probe resolveAndConnect(Endpoint e, long dnsTimeoutMs, long connectTimeoutMs) {
		Resolution r = resolve(e.host(), dnsTimeoutMs);
		if (!r.ok()) {
			return new Probe(e.address(), null, r.durationMs(), r.errorType(), r.error());
		}
		Probe p = connect(e, r.addresses().get(0), connectTimeoutMs);
		return new Probe(p.target(), p.address(), r.durationMs() + p.durationMs(), p.errorType(), p.error());
	}

	/** İstisnanın hata tipi (dns, refused, timeout, noRoute, error). */
	public static String errorType(Throwable t) {
		for (Throwable c = t; c != null; c = c.getCause()) {
			if (c instanceof UnknownHostException) {
				return DNS;
			}
			if (c instanceof NoRouteToHostException) {
				return NO_ROUTE;
			}
			if (c instanceof ConnectException) {
				String m = String.valueOf(c.getMessage()).toLowerCase();
				return m.contains("timed out") ? TIMEOUT : REFUSED;
			}
			if (c instanceof SocketTimeoutException) {
				return TIMEOUT;
			}
			if (c.getCause() == c) {
				break;
			}
		}
		return ERROR;
	}

	/** Hata tipinin Türkçe açıklaması (target: host ya da host:port). */
	public static String explain(String type, String target, long timeoutMs, Throwable cause) {
		return switch (type) {
			case DNS -> "DNS çözümlenemedi: " + target + " (ad yanlış ya da bu pod'un DNS'i bu adı bilmiyor; "
					+ "OpenShift'te Service/Route adını ve namespace'i kontrol edin)";
			case DNS_TIMEOUT -> "DNS sorgusu " + timeoutMs + " ms içinde yanıtlanmadı: " + target
					+ " (DNS sunucusu erişilemiyor ya da yavaş)";
			case REFUSED -> "Bağlantı reddedildi: " + target
					+ " (port kapalı ya da bu adreste dinleyen yok; listeners/port numarasını kontrol edin)";
			case TIMEOUT -> "Bağlantı zaman aşımı: " + target + " (" + timeoutMs
					+ " ms; paketler düşürülüyor olabilir: firewall, NetworkPolicy ya da egress kuralı)";
			case NO_ROUTE -> "Ağ yolu yok: " + target + " (routing/firewall)";
			default -> "Bağlantı hatası: " + target
					+ (cause == null ? "" : " (" + cause.getClass().getSimpleName() + ": " + cause.getMessage() + ")");
		};
	}

	private static long elapsed(long startNanos) {
		return (System.nanoTime() - startNanos) / 1_000_000;
	}

}
