package com.onlinejudge.problem.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.onlinejudge.problem.dto.CreateProblemRequest;
import com.onlinejudge.problem.dto.TestCaseUrlsDto;
import com.onlinejudge.problem.entity.Problem;
import com.onlinejudge.problem.service.ProblemService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Web-layer tests for {@link ProblemController}. The service is mocked; we
 * focus on URL routing, JSON wire shape, status codes and the pretestOnly
 * query parameter.
 *
 * <p>The end-to-end "insert 12 test cases, assert pretest=10 / full=12"
 * lives in {@code ProblemServiceFilteringTest} so it can drive real service +
 * repository mocks together.
 */
@WebMvcTest(ProblemController.class)
class ProblemControllerTest {

    @Autowired private MockMvc mvc;
    @Autowired private ObjectMapper json;
    @MockBean private ProblemService problemService;

    // --- happy path -----------------------------------------------------

    @Test
    void list_returnsArray() throws Exception {
        Problem p = problem("Two Sum", 1000, 256, 100);
        when(problemService.listProblems()).thenReturn(List.of(p));

        mvc.perform(get("/api/v1/problems"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].title").value("Two Sum"))
                .andExpect(jsonPath("$[0].timeLimitMs").value(1000))
                .andExpect(jsonPath("$[0].memoryLimitMb").value(256))
                .andExpect(jsonPath("$[0].points").value(100));
    }

    @Test
    void getById_returns200WhenFound() throws Exception {
        Problem p = problem("Two Sum", 1000, 256, 100);
        when(problemService.getProblem(p.getId())).thenReturn(Optional.of(p));

        mvc.perform(get("/api/v1/problems/{id}", p.getId()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(p.getId().toString()));
    }

    @Test
    void getById_returns404WhenMissing() throws Exception {
        UUID id = UUID.randomUUID();
        when(problemService.getProblem(id)).thenReturn(Optional.empty());

        mvc.perform(get("/api/v1/problems/{id}", id))
                .andExpect(status().isNotFound());
    }

    @Test
    void create_returns201AndBody() throws Exception {
        Problem p = problem("Two Sum", 1000, 256, 100);
        when(problemService.createProblem(any(CreateProblemRequest.class))).thenReturn(p);

        var req = new CreateProblemRequest("Two Sum", 1000, 256, 100);
        mvc.perform(post("/api/v1/problems")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(req)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.title").value("Two Sum"));
    }

    @Test
    void create_validation_rejectsBlankTitle() throws Exception {
        var req = new CreateProblemRequest("", 1000, 256, 100);
        mvc.perform(post("/api/v1/problems")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(req)))
                .andExpect(status().isBadRequest());
    }

    // --- pretestOnly --------------------------------------------------

    @Test
    void getTestCaseUrls_pretestOnly_returnsTenUrls() throws Exception {
        UUID id = UUID.randomUUID();
        when(problemService.getTestCaseUrls(eq(id), eq(true)))
                .thenReturn(buildUrls(10));

        mvc.perform(get("/api/v1/problems/{id}/test-cases", id).param("pretestOnly", "true"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(10))
                .andExpect(jsonPath("$[0].ordinal").value(1))
                .andExpect(jsonPath("$[0].input_url").exists())
                .andExpect(jsonPath("$[0].expected_output_url").exists())
                .andExpect(jsonPath("$[9].ordinal").value(10));
    }

    @Test
    void getTestCaseUrls_full_returnsAll() throws Exception {
        UUID id = UUID.randomUUID();
        when(problemService.getTestCaseUrls(eq(id), eq(false)))
                .thenReturn(buildUrls(12));

        mvc.perform(get("/api/v1/problems/{id}/test-cases", id).param("pretestOnly", "false"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(12))
                .andExpect(jsonPath("$[11].ordinal").value(12));
    }

    @Test
    void getTestCaseUrls_defaultsToFullWhenParamOmitted() throws Exception {
        UUID id = UUID.randomUUID();
        when(problemService.getTestCaseUrls(eq(id), eq(false)))
                .thenReturn(buildUrls(3));

        mvc.perform(get("/api/v1/problems/{id}/test-cases", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(3));
    }

    @Test
    void getTestCaseUrls_unknownProblem_returns404() throws Exception {
        UUID id = UUID.randomUUID();
        when(problemService.getTestCaseUrls(eq(id), any(Boolean.class)))
                .thenThrow(new NoSuchElementException("Problem not found: " + id));

        mvc.perform(get("/api/v1/problems/{id}/test-cases", id))
                .andExpect(status().isNotFound());
    }

    // --- helpers --------------------------------------------------------

    private static Problem problem(String title, int t, int m, int pts) {
        Problem p = new Problem();
        p.setId(UUID.randomUUID());
        p.setTitle(title);
        p.setTimeLimitMs(t);
        p.setMemoryLimitMb(m);
        p.setPoints(pts);
        return p;
    }

    private static List<TestCaseUrlsDto> buildUrls(int n) {
        return java.util.stream.IntStream.rangeClosed(1, n)
                .mapToObj(i -> new TestCaseUrlsDto(
                        i,
                        "https://signed/in" + i,
                        "https://signed/out" + i))
                .toList();
    }
}
