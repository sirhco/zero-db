# Embedding Zero-DB as a Cloud Run Sidecar

> Run Zero-DB **inside the same Cloud Run revision** as your application container. Your app talks to `localhost:8080` instead of a separate service. Same scaling unit, same SA, same bucket — but no extra hop, no separate deploy lifecycle.

This is the path with zero code changes. Both containers ship as one revision. Cloud Run launches them together, scales them together, retires them together.

---

## When to use this

Use a sidecar when:

- **Your app is the only consumer of the KV store.** Deploying Zero-DB as its own service makes sense when many apps share it; embedding makes sense when exactly one does.
- **You want the lifecycle coupled.** A revision rollout updates app + KV together; a rollback rolls both back.
- **Latency matters.** localhost HTTP is ~0.1 ms vs ~5 ms for a separate Cloud Run service.
- **You don't want a second IAM identity to manage.** One SA serves both containers.

Don't use a sidecar when:

- Multiple apps need to share the same KV. Run Zero-DB as a standalone service.
- Your app and KV need independent scaling. Sidecars scale together with the revision.
- Your app is so memory-hungry it'll evict the engine's BlockCache on every request. Run separately.

---

## Architecture

```
┌────────────────────  Cloud Run revision  ────────────────────┐
│                                                              │
│  ┌──────────────┐         localhost:8080      ┌───────────┐  │
│  │     app      │  ────────────────────────▶  │  zero-db  │  │
│  │ (your code)  │                             │           │  │
│  └──────────────┘                             └─────┬─────┘  │
│         ▲                                           │        │
│         │ external HTTPS                            │        │
│         │ (port your app listens on)                │ HTTPS  │
│                                                     ▼        │
└──────────────────────────────────────────────────┬───────────┘
                                                   │
                                                   ▼
                                       ┌─────────────────────┐
                                       │   <BUCKET>          │
                                       │   gs://...          │
                                       │   sstables/, etc.   │
                                       └─────────────────────┘
```

- **Two containers, one revision**: Cloud Run multi-container revisions (GA in 2024).
- **Localhost HTTP**: containers in a revision share the loopback interface. Your app talks to Zero-DB at `http://localhost:<KV_PORT>`.
- **Shared `/tmp`**: WAL on `/tmp/zero-db-wal.log` persists across requests handled by the same instance.
- **One service account**: both containers run under the revision's SA. The same IAM grant on the bucket covers them.

---

## Pre-flight (one-time per project + bucket)

Replace placeholders before running:

- `<PROJECT_ID>` — your GCP project id
- `<REGION>` — e.g. `us-central1`
- `<BUCKET>` — the GCS bucket Zero-DB will read/write
- `<APP_REPO>` — your app's Artifact Registry repo
- `<APP_IMAGE>` — your app's image (e.g. `myapp:1.0.0`)
- `<KV_REPO>` — Zero-DB's Artifact Registry repo (likely `zero-db`)
- `<KV_IMAGE>` — Zero-DB image (likely `zero-db:<sha>`)
- `<SERVICE_NAME>` — Cloud Run service name (your app's name)
- `<RUNTIME_SA>` — dedicated runtime service account email (e.g. `<SERVICE_NAME>-runtime@<PROJECT_ID>.iam.gserviceaccount.com`)
- `<KV_PORT>` — port Zero-DB listens on inside the revision (default `8080`)
- `<APP_PORT>` — port your app listens on, exposed externally by Cloud Run (e.g. `8081`)

```bash
# 1. Enable APIs
gcloud services enable run.googleapis.com artifactregistry.googleapis.com

# 2. Create runtime SA
gcloud iam service-accounts create <SERVICE_NAME>-runtime \
  --project=<PROJECT_ID> \
  --display-name="<SERVICE_NAME> runtime"

# 3. Grant SA access to the bucket
gcloud storage buckets add-iam-policy-binding gs://<BUCKET> \
  --member="serviceAccount:<RUNTIME_SA>" \
  --role="roles/storage.objectAdmin"

# 4. Configure docker auth (if not already)
gcloud auth configure-docker <REGION>-docker.pkg.dev
```

---

## Revision spec

Save as `revision.yaml`. Each `<placeholder>` should be replaced before applying.

```yaml
apiVersion: serving.knative.dev/v1
kind: Service
metadata:
  name: <SERVICE_NAME>
  namespace: <PROJECT_ID>
spec:
  template:
    metadata:
      annotations:
        run.googleapis.com/cpu-throttling: "false"
        run.googleapis.com/execution-environment: gen2
        autoscaling.knative.dev/maxScale: "1"
    spec:
      serviceAccountName: <RUNTIME_SA>
      timeoutSeconds: 60
      containerConcurrency: 80
      containers:

        # ----------------------------------------------------------
        # 1. The app — externally visible. The "ports" entry tells
        #    Cloud Run which container's port to expose to the
        #    outside world.
        # ----------------------------------------------------------
        - name: app
          image: <REGION>-docker.pkg.dev/<PROJECT_ID>/<APP_REPO>/<APP_IMAGE>
          ports:
            - containerPort: <APP_PORT>
          env:
            - name: ZERO_DB_URL
              value: http://localhost:<KV_PORT>
          resources:
            limits:
              memory: 512Mi
              cpu: "1"

        # ----------------------------------------------------------
        # 2. Zero-DB — internal only. No "ports" entry → not exposed.
        #    The app reaches it via localhost.
        # ----------------------------------------------------------
        - name: zero-db
          image: <REGION>-docker.pkg.dev/<PROJECT_ID>/<KV_REPO>/<KV_IMAGE>
          env:
            - name: PORT
              value: "<KV_PORT>"
            - name: GCS_BUCKET
              value: <BUCKET>
            - name: MANIFEST_OBJECT
              value: manifest.json
            - name: WAL_PATH
              value: /tmp/zero-db-wal.log
          resources:
            limits:
              memory: 512Mi
              cpu: "1"
```

Notes on the spec:

- `cpu-throttling: "false"` — keeps Zero-DB's background flush worker making progress between requests. Without this, the compactor stalls when the app is idle.
- `execution-environment: gen2` — required for multi-container revisions and for the `/tmp` tmpfs Zero-DB needs.
- `maxScale: "1"` — single-writer safety. Multi-writer correctness is wired (manifest preconditions via `If-Generation-Match`) but not yet field-validated under contention. Loosen when you have a documented retry-on-`ManifestConflict` client and have run sustained load.
- `containerConcurrency: 80` — both containers handle up to 80 concurrent requests. App tuning, not Zero-DB tuning.
- Per-container `memory: 512Mi` — generous for the typical workload Zero-DB targets (tens of MB of state). Drop to 256Mi if you're tight.

---

## Apply

```bash
gcloud run services replace revision.yaml \
  --region=<REGION> \
  --project=<PROJECT_ID>
```

To allow unauthenticated invocations (public app):

```bash
gcloud run services add-iam-policy-binding <SERVICE_NAME> \
  --region=<REGION> \
  --member="allUsers" \
  --role="roles/run.invoker"
```

If your app is internal-only, skip that step and rely on whatever IAP / IAM gating you already use.

---

## Verifying the deploy

```bash
URL=$(gcloud run services describe <SERVICE_NAME> \
  --region=<REGION> --format='value(status.url)')

# Sanity: app responds
curl -i "${URL}/healthz"   # or whatever your app exposes

# Confirm both containers booted by tailing logs
gcloud run services logs read <SERVICE_NAME> --region=<REGION> --limit=40
```

You should see startup lines from both containers. Zero-DB prints:

```
persistence: GCS bucket=<BUCKET>, manifest=manifest.json, wal=/tmp/zero-db-wal.log
zero-db listening on 0.0.0.0:<KV_PORT>
zero-db ready (engine init complete)
```

If you see `loadFromManifest: rangeGet failed: AuthFailed — proceeding as fresh bucket`, the SA is set up correctly but IAM hasn't propagated yet (give it ~30 seconds). After it propagates, no more such lines.

---

## Verifying KV-side state

From your laptop with the bucket's IAM grant on your user account:

```bash
gcloud storage ls gs://<BUCKET>/
gcloud storage cat gs://<BUCKET>/manifest.json
```

After the app exercises the KV path, you should see `sstables/000000.sst` plus a `manifest.json` listing it.

---

## Limitations

- **Localhost only.** The app cannot connect to the KV from outside the revision. If you need a debug endpoint for KV state, expose it through the app, not directly.
- **Shared filesystem under `/tmp`.** The WAL lives there. Don't have your app write to the same path.
- **One revision = one writer.** With `maxScale: 1` you guarantee single-writer semantics across the whole bucket. Bumping it requires the multi-writer correctness story to be field-tested first.
- **Cold start adds Zero-DB init cost.** Cloud Run scales-to-zero pulls both images on first request. Budget a few hundred ms for the first response.
- **Per-container resource limits.** `memory: 512Mi` is per container; the revision allocates 1 GiB total. Account for this when sizing the instance.

---

## Cost model

Identical to running Zero-DB standalone:

- Cloud Run compute — billed per request-second on the entire revision (both containers count).
- GCS storage — flat per GiB.
- GCS Class-A operations — once per flush + manifest rewrite.

The sidecar pattern is **cheaper** than two separate Cloud Run services because you pay for one revision's instance time, not two.

---

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| `compaction_failures` count climbing in `/admin/stats` | Logs include `compaction failed: <ErrorName>` — usually `AuthFailed` (SA / IAM). |
| `AuthFailed` on every boot, never recovers | Runtime SA not bound on the bucket, or token-scope issue. Check `gcloud storage buckets get-iam-policy gs://<BUCKET>`. |
| Container fails startup probe | Logs show no `zero-db listening` line — Zero-DB image is missing or unreachable. Check Artifact Registry pull permissions for the SA. |
| App hangs trying to reach `localhost:<KV_PORT>` | Zero-DB container crashed or never started. Pull container-specific logs: `gcloud run services logs read <SERVICE_NAME> --region=<REGION> --container zero-db`. |

---

## Related documents

- [`docs/OVERVIEW.md`](OVERVIEW.md) — what Zero-DB is, when to use it.
- [`deploy/Dockerfile`](../deploy/Dockerfile) — how the Zero-DB image is built.
- [`deploy/deploy.sh`](../deploy/deploy.sh) — standalone (non-sidecar) deploy.
