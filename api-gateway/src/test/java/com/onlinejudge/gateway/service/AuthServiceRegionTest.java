package com.onlinejudge.gateway.service;

import com.onlinejudge.gateway.dto.LoginRequest;
import com.onlinejudge.gateway.dto.SignupRequest;
import com.onlinejudge.gateway.dto.TokenPairResponse;
import com.onlinejudge.gateway.entity.AuthEvent;
import com.onlinejudge.gateway.entity.RefreshToken;
import com.onlinejudge.gateway.entity.User;
import com.onlinejudge.gateway.repository.AuthEventRepository;
import com.onlinejudge.gateway.repository.RefreshTokenRepository;
import com.onlinejudge.gateway.repository.UserRepository;
import com.onlinejudge.gateway.security.JwtTokenProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link AuthService}'s region-aware behaviour:
 *
 * <ul>
 *   <li>Signup stamps {@code user.region} with the current gateway's region.</li>
 *   <li>Login reads {@code user.region} and embeds it as the JWT region claim.</li>
 *   <li>Refresh preserves the region claim across token rotation.</li>
 * </ul>
 *
 * Repositories are Mockito mocks backed by in-memory maps, mirroring
 * {@code AuthServiceTest}'s seam pattern.
 */
class AuthServiceRegionTest {

    private static final String SECRET = "test-secret-32-bytes-minimum-aaaaaaaaaa";
    private static final String GATEWAY_REGION = "asia-south1";

    private final Map<UUID, User> usersById = new HashMap<>();
    private final Map<String, User> usersByName = new HashMap<>();
    private final Map<String, RefreshToken> refreshByHash = new HashMap<>();
    private final List<AuthEvent> authEvents = new ArrayList<>();

    private UserRepository userRepository;
    private RefreshTokenRepository refreshTokenRepository;
    private AuthEventRepository authEventRepository;
    private JwtTokenProvider tokenProvider;
    private AuthService authService;

    @BeforeEach
    void setUp() {
        usersById.clear();
        usersByName.clear();
        refreshByHash.clear();
        authEvents.clear();

        userRepository = mock(UserRepository.class);
        refreshTokenRepository = mock(RefreshTokenRepository.class);
        authEventRepository = mock(AuthEventRepository.class);

        when(userRepository.existsByUsername(anyString()))
                .thenAnswer(inv -> usersByName.containsKey(inv.<String>getArgument(0)));
        when(userRepository.findByUsername(anyString()))
                .thenAnswer(inv -> Optional.ofNullable(usersByName.get(inv.<String>getArgument(0))));
        when(userRepository.findById(any(UUID.class)))
                .thenAnswer(inv -> Optional.ofNullable(usersById.get(inv.<UUID>getArgument(0))));
        when(userRepository.save(any(User.class))).thenAnswer(inv -> {
            User u = inv.getArgument(0);
            usersById.put(u.getId(), u);
            usersByName.put(u.getUsername(), u);
            return u;
        });

        when(refreshTokenRepository.findByTokenHash(anyString()))
                .thenAnswer(inv -> Optional.ofNullable(refreshByHash.get(inv.<String>getArgument(0))));
        when(refreshTokenRepository.save(any(RefreshToken.class))).thenAnswer(inv -> {
            RefreshToken rt = inv.getArgument(0);
            refreshByHash.put(rt.getTokenHash(), rt);
            return rt;
        });
        when(refreshTokenRepository.revoke(anyString(), any(Instant.class))).thenAnswer(inv -> {
            String hash = inv.getArgument(0);
            Instant when = inv.getArgument(1);
            RefreshToken t = refreshByHash.get(hash);
            if (t == null || t.getRevokedAt() != null) return 0;
            t.setRevokedAt(when);
            return 1;
        });

        when(authEventRepository.save(any(AuthEvent.class))).thenAnswer(inv -> {
            AuthEvent e = inv.getArgument(0);
            authEvents.add(e);
            return e;
        });

        tokenProvider = new JwtTokenProvider(SECRET, 1, "online-judge");
        authService = new AuthService(userRepository, refreshTokenRepository, authEventRepository,
                tokenProvider, 7 * 24 * 3600L, GATEWAY_REGION);
    }

    @Test
    void signup_stampsCurrentGatewayRegionOnUser() {
        UUID userId = authService.signup(signup("alice", "supersecret123"), "1.2.3.4", "ua");

        User stored = usersById.get(userId);
        assertThat(stored).isNotNull();
        assertThat(stored.getRegion()).isEqualTo(GATEWAY_REGION);
    }

    @Test
    void login_embedsUserRegionInAccessToken() {
        authService.signup(signup("alice", "supersecret123"), "1.2.3.4", "ua");
        TokenPairResponse pair = authService.login(login("alice", "supersecret123"), "1.2.3.4", "ua");

        assertThat(pair.getAccessToken()).isNotBlank();
        assertThat(tokenProvider.extractRegion(pair.getAccessToken())).isEqualTo(GATEWAY_REGION);
    }

    @Test
    void login_pullsRegionFromStoredUser_notFromGatewayDefault() {
        // Construct a user signed up in a DIFFERENT region (simulating: a
        // user who originally registered in us-central1 now logs in via an
        // asia-south1 gateway). Login must read region off the User row.
        UUID id = UUID.randomUUID();
        User other = new User(id, "bob", encodeFor("hunter2hunter2"), "us-central1");
        usersById.put(id, other);
        usersByName.put("bob", other);

        TokenPairResponse pair = authService.login(login("bob", "hunter2hunter2"), "1.2.3.4", "ua");

        assertThat(tokenProvider.extractRegion(pair.getAccessToken())).isEqualTo("us-central1");
    }

    @Test
    void refresh_preservesRegionClaimAcrossRotation() {
        authService.signup(signup("alice", "supersecret123"), "1.2.3.4", "ua");
        TokenPairResponse first = authService.login(login("alice", "supersecret123"), "1.2.3.4", "ua");

        TokenPairResponse rotated = authService.refresh(first.getRefreshToken(), "1.2.3.4", "ua");

        // The two access tokens may be byte-identical when issued in the same
        // second (JWT iat/exp are second-granularity) — that's covered by the
        // dedicated rotation test in AuthServiceTest. Here we only assert the
        // region claim survives the rotation.
        assertThat(tokenProvider.extractRegion(rotated.getAccessToken())).isEqualTo(GATEWAY_REGION);
    }

    private static SignupRequest signup(String username, String password) {
        SignupRequest r = new SignupRequest();
        r.setUsername(username);
        r.setPassword(password);
        return r;
    }

    private static LoginRequest login(String username, String password) {
        LoginRequest r = new LoginRequest();
        r.setUsername(username);
        r.setPassword(password);
        return r;
    }

    /**
     * Mirror AuthService's Argon2id encoder so a hand-built user row has a
     * verifiable password hash without exposing the real encoder.
     */
    private static String encodeFor(String raw) {
        return org.springframework.security.crypto.argon2.Argon2PasswordEncoder
                .defaultsForSpringSecurity_v5_8().encode(raw);
    }
}
