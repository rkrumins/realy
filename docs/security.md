# Security

realy is a benchmarking instrument, not a payment system. All data is synthetic, and the services
assume a trusted network: the gateway and model admin endpoints and Kafka, Redis and ClickHouse have
no authentication. The console is the one component meant to face people, and its access control is
simple and optional. Read this before exposing anything beyond your machine.

## Console token login

Set `REALY_CONSOLE_TOKEN` to require a token:

```bash
REALY_CONSOLE_TOKEN=$(openssl rand -hex 24) make up                          # Docker
kubectl -n realy create secret generic realy-console --from-literal=token=... # Kubernetes
kubectl -n realy rollout restart deploy/console
```

With a token set:

| Surface | Accepts |
|---|---|
| `/api/*` (except `/api/health`, `/api/auth/*`) | `Authorization: Bearer <token>` or the session cookie; otherwise `401 {"detail":"unauthorized"}` |
| WebSocket `/api/live` | session cookie, `Authorization: Bearer`, or `?token=<token>`; otherwise closed with code 4401 |
| UI | the sign-in page; `POST /api/auth/login {"token": ...}` sets the session cookie |

* **Session cookie:** `realy_session`, an HMAC-SHA256 of the token.
  * It is `HttpOnly` and `SameSite=Strict`, and `Secure` when the request arrived over HTTPS.
  * It lasts 12 hours, and the token itself is never stored in the browser.
* **Comparisons** are constant-time.
* **Rotating the token** invalidates every session.
* **One token, one role:** there are no users. Anyone with the token can start, abort and
  reconfigure runs.
* **Without a token** the console is open, and the UI says so on the Settings page.

Scripts and CI use the Bearer header:

```bash
curl -H "Authorization: Bearer $REALY_CONSOLE_TOKEN" http://localhost:8090/api/runs
```

## Behind an ingress or SSO

For anything shared, put the console behind your organisation's authentication:
* an OAuth2 proxy;
* Google IAP;
* Cloudflare Access;
* an ingress with external auth.

Add the token as a second factor, or as the only factor on a private network.
`deploy/k8s/components/console-ingress` provides an Ingress with WebSocket-friendly timeouts. When
TLS terminates at the ingress, set `FORWARDED_ALLOW_IPS` on the console to the ingress address range
(or `*` inside a private cluster). Uvicorn then trusts `X-Forwarded-Proto`, and the session cookie
gets the `Secure` flag.

## Network exposure

* **Docker Compose** publishes every service port on all host interfaces: Kafka 9092, Redis 6379,
  ClickHouse 8123/9000, gateway 8080, model 8001/8002, Flink 8081 and the console 8090. That suits
  a laptop. On a shared host, bind them to loopback (`"127.0.0.1:8090:8090"`) or drop the `ports:`
  of everything but the console.
* **Kubernetes:** every Service is `ClusterIP`. The `local` overlay is the exception: it exposes
  the console and the Flink UI as NodePorts for k3d/kind. Reach the console through
  `kubectl port-forward` or the ingress component. Consider a NetworkPolicy that admits only the
  console's namespace peers to the admin ports (gateway 8080 `/admin/*`, model 8002).
* The admin endpoints (`POST /admin/config`) accept runtime configuration from anyone who can
  reach them. That is how the console reconfigures a run, so keep them internal.

## Build and supply chain

* Images are built from source in this repository and pinned base images. `docker compose build`
  needs network access to Docker Hub, Maven Central (or `MAVEN_MIRROR`), npm and PyPI.
* A proxy CA passed to the build ([deployment-docker.md](deployment-docker.md)) is a BuildKit secret.
  It is used during the build and never written to a layer.
* No credentials are baked into images or manifests. The console token comes from the environment
  or a Kubernetes Secret, and the manifests reference that Secret with `optional: true`.
* Containers run as their base image's default user: root for the console, gateway and model;
  `flink` (uid 9999, after the entrypoint) for Flink; `appuser` for Kafka. For hardened clusters,
  add `runAsNonRoot` and read-only root filesystems in an overlay. The console needs `/runs`,
  `/data` and `/tmp` writable.

## Data

All payments, customers, devices and fraud cases are generated from a seed
([synthetic-data.md](synthetic-data.md)), so there is no personal data. ClickHouse stores every
decision with its inputs for audit and training. Treat a ClickHouse instance fed by a *real*
pipeline as sensitive; the schema is production-shaped on purpose.
