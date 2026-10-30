# Cloud Run deployment

## Prerequisites

Create a Google Cloud project and link billing. Cloud Run free usage does not
guarantee a zero bill: builds, image storage, secrets, and network traffic can
have separate charges. Configure a billing budget alert (an alert is not a hard cap).
Install the Google Cloud CLI and authenticate with `gcloud auth login`.

Use region `us-east1`, minimum instances 0, maximum instances 1, request-based
CPU billing, 1 CPU and 512 MiB memory for the initial API deployment.
Use a dedicated runtime service account with access only to the database secret.
Store DATABASE_URL in Secret Manager; never include `.env` in a build/upload.

## Local container

From the workspace root:

```bash
docker build -t market-query-api:local .
docker run --rm --env-file .env -e PORT=8080 -p 8080:8080 market-query-api:local
```

Check `/ready`, `/prices/{symbol}`, `/trades/{symbol}`, and `/alerts`.
The image runs as UID 10001 and includes system CA certificates for Neon TLS.
The build context allowlist includes only Cargo manifests, lockfile and crates.

## Cloud setup (after choosing PROJECT_ID)

Enable Cloud Run, Artifact Registry, Cloud Build, and Secret Manager APIs.
Create a Docker Artifact Registry repository, build/push the tested image, and
create the runtime service account. Create a database secret from stdin and
grant that runtime account roles/secretmanager.secretAccessor on that secret only.
Pin a numeric secret version when deploying.

Deploy with `--min-instances=0 --max-instances=1 --cpu=1 --memory=512Mi
--concurrency=20 --timeout=30 --cpu-throttling` and supply DATABASE_URL using
`--set-secrets=DATABASE_URL=SECRET_NAME:VERSION`. Set CORS_ALLOWED_ORIGINS to
the dashboard origin(s). Initially use localhost:5173 for browser testing.
The dashboard needs public GET endpoints; enabling unauthenticated invocation
will expose stored market data through the read-only routes.

After deployment, verify the HTTPS `/ready` endpoint and data routes.
No Kafka or Redis is needed by this container. The local consumer continues
to feed Neon independently.

## Deployed environment

- Project: `project-68f3ad8b-ff14-410c-b04`
- Region: `us-east1`
- Service: `market-query-api`
- URL: https://market-query-api-174136811681.us-east1.run.app
- Artifact Registry: `market-pipeline/query-api:initial`
- Runtime identity: `market-query-api@project-68f3ad8b-ff14-410c-b04.iam.gserviceaccount.com`
- DATABASE_URL: Secret Manager `market-api-database-url`, version `1`
- Allowed browser origin: `http://localhost:5173` (add Vercel origin when deployed)
- Minimum instances 0; maximum instances 1; concurrency 20; request timeout 30s.

No secret values are stored in this guide or the image.
