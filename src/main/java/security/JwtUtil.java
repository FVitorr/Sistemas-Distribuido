package security;

import io.jsonwebtoken.*;
import io.jsonwebtoken.security.Keys;

import java.security.Key;
import java.util.Base64;
import java.util.Date;

public class JwtUtil {

    // Chave secreta segura (Base64 → Key)
    private static final String SECRET_STRING = "b0f3N2p7dFw4dVZkYmJrM3Z5R2Z6cVJ6d0R4cE5sY1U=";

    private static final Key SECRET_KEY =
            Keys.hmacShaKeyFor(Base64.getDecoder().decode(SECRET_STRING));

    // Tempo do token (30 minutos)
    private static final long EXPIRATION = 1000 * 60 * 30;

    // Gerar token
    public static String gerarToken(String username) {
        return Jwts.builder()
                .setSubject(username)
                .setIssuedAt(new Date())
                .setExpiration(new Date(System.currentTimeMillis() + EXPIRATION))
                .signWith(SECRET_KEY, SignatureAlgorithm.HS256)
                .compact();
    }

    // Validar token
    public static String validarToken(String token) throws Exception {
        Claims claims = Jwts.parserBuilder()
                .setSigningKey(SECRET_KEY)
                .build()
                .parseClaimsJws(token)
                .getBody();

        if (claims.getExpiration().before(new Date())) {
            throw new Exception("Token expirado");
        }

        return claims.getSubject();
    }

    // Extrair username
    public static String extrairUsername(String token) {
        return Jwts.parserBuilder()
                .setSigningKey(SECRET_KEY)
                .build()
                .parseClaimsJws(token)
                .getBody()
                .getSubject();
    }
}
