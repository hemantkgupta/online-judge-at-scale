package com.onlinejudge.gateway.service;

import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link RegionResolver} — the per-request region picker that
 * stamps {@code submissions.region} and {@code outbox_events.region}.
 *
 * <p>Properties verified:
 * <ul>
 *   <li>{@code X-Region} header wins over the config default — required for
 *       multi-region simulation tests where a single gateway pod handles
 *       traffic claiming different origin regions.</li>
 *   <li>Missing/blank header falls back to {@code app.region}.</li>
 *   <li>A {@code null} request also falls back to the default — internal
 *       call sites (schedulers, async jobs) don't carry a request context.</li>
 *   <li>The constructor rejects a blank default — fail-fast on misconfig.</li>
 * </ul>
 */
class RegionResolverTest {

    @Test
    void resolve_headerOverridesDefault() {
        RegionResolver resolver = new RegionResolver("us-east-1");
        MockHttpServletRequest req = new MockHttpServletRequest();
        req.addHeader(RegionResolver.REGION_HEADER, "eu-west-1");

        assertThat(resolver.resolve(req)).isEqualTo("eu-west-1");
    }

    @Test
    void resolve_trimsHeaderValue() {
        RegionResolver resolver = new RegionResolver("us-east-1");
        MockHttpServletRequest req = new MockHttpServletRequest();
        req.addHeader(RegionResolver.REGION_HEADER, "  ap-south-1  ");

        assertThat(resolver.resolve(req)).isEqualTo("ap-south-1");
    }

    @Test
    void resolve_missingHeaderFallsBackToDefault() {
        RegionResolver resolver = new RegionResolver("us-east-1");
        MockHttpServletRequest req = new MockHttpServletRequest();

        assertThat(resolver.resolve(req)).isEqualTo("us-east-1");
    }

    @Test
    void resolve_blankHeaderFallsBackToDefault() {
        RegionResolver resolver = new RegionResolver("us-east-1");
        MockHttpServletRequest req = new MockHttpServletRequest();
        req.addHeader(RegionResolver.REGION_HEADER, "   ");

        assertThat(resolver.resolve(req)).isEqualTo("us-east-1");
    }

    @Test
    void resolve_nullRequestFallsBackToDefault() {
        RegionResolver resolver = new RegionResolver("us-east-1");
        HttpServletRequest req = null;

        assertThat(resolver.resolve(req)).isEqualTo("us-east-1");
    }

    @Test
    void defaultRegion_returnsConfiguredValue() {
        RegionResolver resolver = new RegionResolver("eu-west-1");

        assertThat(resolver.defaultRegion()).isEqualTo("eu-west-1");
    }

    @Test
    void constructor_rejectsBlankDefault() {
        assertThatThrownBy(() -> new RegionResolver(""))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RegionResolver("   "))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RegionResolver(null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
