-- =============================================================================
-- V7: contest_problems join table.
--
-- Roadmap §4.23: system-test replay needs to know which problems belong to a
-- contest so that when contest-service flips the contest to CLOSED it can walk
-- every ACCEPTED pretest submission for those problems and re-publish them on
-- `submissions.system` for the full system-test suite.
--
-- Pre-V7 there was no contest→problem linkage in the shared DB. Submissions
-- carried a `contest_id` (so we could find submissions for a contest), but
-- there was no way to enumerate "the set of problems that belong to contest X"
-- without going through every submission ever made for it. That works for the
-- single-problem demo contest, but it's both slow and wrong for any contest
-- where a problem is added but never submitted to — those still need their
-- accepted submissions from other contests checked? No — replay is scoped to
-- the *problems of this contest*, so we need an explicit membership table.
--
-- Design:
--   * Many-to-many: a problem can appear in multiple contests (re-use across
--     practice rounds), and a contest has many problems (typical: 6-8).
--   * Composite PK (contest_id, problem_id) prevents duplicate enrollment.
--   * `ordinal` gives stable problem ordering (A=1, B=2, …) for the UI.
--   * `points` is denormalized off problems.points so a contest can weight
--     the same problem differently than its default score.
--   * `idx_contest_problems_contest (contest_id, ordinal)` is the lookup
--     index for "list problems of contest X in display order" — also the
--     scan path for the system-test replay publisher.
-- =============================================================================

CREATE TABLE IF NOT EXISTS contest_problems (
    contest_id UUID NOT NULL REFERENCES contests(id),
    problem_id UUID NOT NULL,
    ordinal    INT  NOT NULL,
    points     INT  NOT NULL DEFAULT 100,
    PRIMARY KEY (contest_id, problem_id)
);

CREATE INDEX IF NOT EXISTS idx_contest_problems_contest
    ON contest_problems (contest_id, ordinal);
