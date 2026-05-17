# CI/CD

Three workflows live under `.github/workflows/`:

| Workflow              | Triggers                              | What it does                                                                 |
|-----------------------|---------------------------------------|------------------------------------------------------------------------------|
| `ci.yml`              | `pull_request` to `main`; `push` to any non-`main` branch | actionlint, `:<module>:test` matrix over all 9 Gradle modules, proto roundtrip, `tofu validate`, Go agent `build + test`. Cancels superseded PR runs. |
| `build-and-push.yml`  | `push` to `main`                      | WIF auth to GCP, `docker buildx` each service Dockerfile, push `<short-sha>` + `latest` tags to Artifact Registry (`asia-south1-docker.pkg.dev/<project>/oj/<service>`). |
| `deploy.yml`          | `workflow_dispatch` only              | Manual rollout to `oj-control-plane` and/or `oj-compute` over IAP-tunnelled SSH. Inputs: `tag` (default `latest`), `target` (`compute` / `control-plane` / `both`). Post-deploy `/actuator/health` check on every service. |

## Reading failures

- A red check on a PR points to a job in `ci.yml`. Click "Details" to open
  the job log. The `gradle-test` matrix uploads `test-reports-<module>`
  artifacts on failure — download those for the surefire HTML output.
- A red check after merge points to `build-and-push.yml`. Most failures
  here are WIF / IAM regressions (see below).

## Re-running

- Failed CI run: open the run, click "Re-run failed jobs". For a fresh
  cache hit, push an empty commit (`git commit --allow-empty -m 'retry ci'`).
- Failed build-and-push: re-run from the GitHub UI; the build cache is
  scoped per image so a single flaky cell re-runs cheaply.

## Manually dispatching a deploy

1. GitHub UI -> Actions -> "Deploy" -> "Run workflow".
2. Pick branch `main`, set `tag` (paste the 12-char SHA from the
   `build-and-push` run, or leave `latest`), set `target`.
3. Watch the job. The health-check step is the final gate — if it red-bars,
   SSH into the target VM and inspect `docker compose ps` / `logs`.

## Required repository secrets

Configure under Settings -> Secrets and variables -> Actions:

| Secret                              | Used by                                  | Value                                                                 |
|-------------------------------------|------------------------------------------|-----------------------------------------------------------------------|
| `GCP_PROJECT_ID`                    | `build-and-push.yml`, `deploy.yml`       | e.g. `online-judge-prod`                                              |
| `GCP_WORKLOAD_IDENTITY_PROVIDER`    | `build-and-push.yml`, `deploy.yml`       | Full WIF provider resource name (see below)                           |
| `GCP_DEPLOY_SERVICE_ACCOUNT`        | `build-and-push.yml`, `deploy.yml`       | Email of the SA that GitHub assumes (e.g. `gha-deployer@<proj>.iam.gserviceaccount.com`) |

No static service-account JSON key. No SSH private key — IAP brokers the SSH session.

## One-time WIF setup (operator)

Run once, after the GCP project is created:

```bash
PROJECT_ID=online-judge-prod
PROJECT_NUMBER=$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)')
POOL=github-pool
PROVIDER=github-provider
SA=gha-deployer@${PROJECT_ID}.iam.gserviceaccount.com
REPO=hemantkgupta/online-judge-at-scale  # owner/repo

gcloud iam workload-identity-pools create "$POOL" \
  --location=global --display-name="GitHub Actions pool"

gcloud iam workload-identity-pools providers create-oidc "$PROVIDER" \
  --location=global --workload-identity-pool="$POOL" \
  --issuer-uri="https://token.actions.githubusercontent.com" \
  --attribute-mapping="google.subject=assertion.sub,attribute.repository=assertion.repository" \
  --attribute-condition="assertion.repository=='${REPO}'"

gcloud iam service-accounts create gha-deployer \
  --display-name="GitHub Actions deployer"

gcloud iam service-accounts add-iam-policy-binding "$SA" \
  --role=roles/iam.workloadIdentityUser \
  --member="principalSet://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${POOL}/attribute.repository/${REPO}"

# Roles the SA needs:
for role in roles/artifactregistry.writer roles/compute.instanceAdmin.v1 \
            roles/iap.tunnelResourceAccessor roles/iam.serviceAccountUser; do
  gcloud projects add-iam-policy-binding "$PROJECT_ID" --member="serviceAccount:$SA" --role="$role"
done

echo "GCP_WORKLOAD_IDENTITY_PROVIDER=projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${POOL}/providers/${PROVIDER}"
echo "GCP_DEPLOY_SERVICE_ACCOUNT=$SA"
```

Paste the two echoed values plus `GCP_PROJECT_ID` into repo secrets. Done.

## Local linting

Maintainers can validate workflows before pushing:

```bash
./infra/scripts/lint-workflows.sh
```

Requires `actionlint` 1.7.x on `$PATH` (`brew install actionlint` or
`go install github.com/rhysd/actionlint/cmd/actionlint@latest`).
