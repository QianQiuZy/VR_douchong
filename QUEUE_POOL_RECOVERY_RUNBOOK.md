# Queue pool recovery deployment runbook

This runbook covers the production deployment gates for the report connection-pool recovery and the optional EdgeOne cache rules. It is an operator procedure, not evidence that either the application release or EdgeOne console configuration has been deployed.

## Current evidence and deployment gate

The available checks observed HTTP/2 500 responses with `eo-cache-status: MISS` from `vr.qianqiuzy.cn` and `psp.qianqiuzy.cn`. Both hostnames use CNAMEs under `*.eo.dnse0.com`. These observations show EdgeOne is in the request path. They do not identify the configured origin for either Host, prove either hostname maps to this service on `APP_PORT=4666`, establish which `/gift*` paths share an origin, or show that any cache rule is deployed.

Before changing EdgeOne, an operator with access to the authenticated EdgeOne console and origin inventory must record, for each hostname:

| Host | Configured EdgeOne origin and port | Origin application/service | `/gift*` path ownership and collisions | Evidence and time |
| --- | --- | --- | --- | --- |
| `vr.qianqiuzy.cn` | Confirm in console | Confirm with service owner | Enumerate all paths and owners | Record here in change evidence |
| `psp.qianqiuzy.cn` | Confirm in console | Confirm with service owner | Enumerate all paths and owners | Record here in change evidence |

**Stop** if a hostname's origin or any relevant path ownership is unknown, if another backend owns a candidate path, or if this application's origin cannot be distinguished. Do not infer topology from DNS, a public response, the port number, or a matching path name. Apply rules only to confirmed host and path combinations. If neither hostname is confirmed for this service, make no EdgeOne changes.

## Intended EdgeOne rule matrix

Configure these only after the topology gate passes and the matching application release is healthy. Use exact path matching, not a prefix or wildcard rule. Scope the rule to the confirmed Host and method `GET`.

| Exact path | Method | Cache policy | Query key behavior |
| --- | --- | --- | --- |
| `/gift` | GET only | Force Cache, TTL 5 seconds | Preserve EdgeOne's default query-string cache key behavior |
| `/gift/by_month` | GET only | Force Cache, TTL 5 seconds | Preserve `month` in the cache key using default behavior; no `room_id` key |
| `/gift/live_sessions` | GET only | Force Cache, TTL 5 seconds | Preserve both `month` and `room_id` in the cache key using default behavior |
| `/gift/attention` | GET only | Force Cache, TTL 5 seconds | Preserve both `month` and `room_id` in the cache key using default behavior |

Do not add a custom cache key that ignores, drops, or normalizes away query parameters. In particular, different `month` values must not share a key. If the provider's default query-key behavior cannot be confirmed, stop rather than enable a rule that could cross-serve parameterized results.

Keep `/gift/sc` uncached. Its response includes `uname`, `uid`, and `message`, and this runbook does not authorize caching it. Keep `/add/room` and `/delete/room` uncached. Do not create a rule for any POST request, wildcard method, user-specific or authenticated response, or any other path. The origin application must remain cache-neutral. Do not add origin `Cache-Control`, `s-maxage`, or similar directives as part of this procedure.

### Status code policy

EdgeOne's documented content-cache behavior permits caching responses with status 200 or 206 when a matching cache rule applies. Its documented default is to cache 404 responses for 10 seconds, while other statuses are not cached by default. The status-code TTL feature supports configuring 503. These are provider defaults, not proof of the current zone settings.

Before enabling Force Cache, inspect the zone's status-code TTL configuration and set the TTL to zero or otherwise explicitly disable caching for 404 and 503 if the console supports that policy. Leave 400 and 500 uncached, and do not add status-code rules that cache them. If the default 404/10-second behavior cannot be overridden, document and explicitly accept that behavior before rollout; otherwise do not enable the rules. A 404 may be cached by that default even though this runbook creates no explicit 404 rule. A 503 must never be cached. Verify these settings and the observed response headers during the canary. Never interpret an error response as a successful cache hit.

## Pre-deployment checks

1. Pass and record the topology gate above for every Host that will receive rules. Review all co-hosted `/gift*` paths and exclude routes that belong to another service.
2. Deploy and verify the application changes independently of EdgeOne. Use the repository's existing production launcher and configuration. Keep one production process and one Uvicorn worker. Do not add workers to increase capacity.
3. Check the service's health and the four approved GET routes directly through the normal production path. Use known-safe, non-sensitive query values. Do not use real account actions or mutation requests as a cache test.
4. Confirm the application returns its expected successful responses before enabling caching. If the origin is unhealthy, returns unexpected 5xx responses, or logs pool timeouts, stop and investigate. Do not mask an origin failure with Force Cache.
5. Record the current EdgeOne rule and status-code TTL settings so they can be restored. Do not record credentials, cookies, response bodies, or sensitive query values in change evidence.

## Header-only verification

The examples below are commands for the authorized operator to run after the topology gate. They are not commands already executed as part of preparing this document. They discard response bodies and print response headers, including status and `EO-Cache-Status` when present. Do not add `-v`, `--trace`, or commands that print response bodies.

Set the confirmed hostname and choose a harmless known-good month accepted by the application. Do not put credentials or personal data in the URL.

```sh
HOST=confirmed-hostname.example
MONTH=2026-08
ROOM_ID=123456

curl -sS -D - -o /dev/null "https://${HOST}/gift"
curl -sS -D - -o /dev/null "https://${HOST}/gift/by_month?month=${MONTH}"
curl -sS -D - -o /dev/null "https://${HOST}/gift/live_sessions?room_id=${ROOM_ID}&month=${MONTH}"
curl -sS -D - -o /dev/null "https://${HOST}/gift/attention?room_id=${ROOM_ID}&month=${MONTH}"
```

For each approved route, send two identical GET requests through the confirmed EdgeOne Host within five seconds. Check only status and headers. The first request should be a MISS or a provider-equivalent uncached status; the following request should show `EO-Cache-Status: HIT` while still inside the five-second TTL. Header names and provider values may vary by configuration, so record the exact observed values rather than assuming a hit.

For all three parameterized paths, verify the `month` cache-key dimension: request the known-good month twice, then a different valid month twice. For `/gift/live_sessions` and `/gift/attention`, also verify the `room_id` cache-key dimension by holding the known-good month fixed, requesting `room_id=123456` twice, then a different valid positive room ID twice. For each distinct key, the first request should be a MISS (or provider-equivalent uncached status) and the repeat should be a HIT within the TTL. Do not treat a hit for one key value as evidence for another, and do not inspect or save response bodies to establish isolation.

Use header-only requests to check error and exclusion behavior:

```sh
# Malformed month should be an error response, not a cacheable successful response.
curl -sS -D - -o /dev/null "https://${HOST}/gift/by_month?month=not-a-month"

# Check only if this exact path is known to belong to this origin.
curl -sS -D - -o /dev/null "https://${HOST}/gift/sc?room_id=${ROOM_ID}"

# A deliberately unknown path observes the zone's documented/default 404 policy.
curl -sS -D - -o /dev/null "https://${HOST}/runbook-404-probe"
```

Do not issue a POST to a production mutation endpoint for cache testing. Confirm POST exclusions from the exact method match in the console. A safe 503 probe may be used only in an approved non-production environment or through an existing controlled test that does not mutate production state. Do not induce production overload to test 503 caching. Verify the explicit 503 TTL policy in the console and record any naturally observed production 503 only by status and headers.

Record Host, exact path, method, query key category (not sensitive values), UTC time, status, and cache-status headers. Never record response bodies. If a request produces an unexpected status, cache hit on an error, cross-key hit, or missing headers that prevent verification, disable the rule and investigate.

## Single-process production rollout

1. Schedule the release using the service owner's existing deployment procedure. Preserve the repository's production launcher order and its single-process runtime boundary. Keep one production API worker. Do not start a second worker, the local read-only QA server, or a second copy of the monitor as part of this runbook.
2. Deploy the reviewed application release and configured non-secret pool/admission settings using the existing service deployment mechanism. Do not print or copy environment values. Keep existing settings unless the approved release explicitly changes them based on measured capacity evidence.
3. Restart the single existing production process once, using the established service manager procedure. Confirm it binds the configured API port and passes health and approved GET checks. Observe pool-timeout and report-admission errors using the service's normal redacted logging. Stop if the service fails, unexpected 5xx responses appear, or pool counters do not recover.
4. Once origin health is confirmed and topology is recorded, enable the exact-path rules for one confirmed Host as a canary. Verify the first MISS and repeated HIT for all four routes, query isolation, status policy, and excluded paths using the header-only checks above.
5. Observe the canary for the service owner's approved window. Track response statuses, cache-status headers, and redacted pool/admission health. Do not claim the WebSocket reset symptom is fixed by CDN caching. If the canary is healthy, apply the same verified rule matrix to the next confirmed Host, if any, and repeat verification.
6. Record which rules were actually enabled, their exact Host/path/method scope, the verified status policy, timestamps, header-only results, and any unresolved topology or error behavior. Do not describe a planned rule as deployed.

## Rollback

Disable or remove the newly added exact-path rule in EdgeOne first. Do not rely on purge as the primary rollback for a five-second TTL. Wait six seconds from the disable action so any object already served under the rule can expire naturally, then repeat a header-only GET to each affected approved path. Confirm it no longer reports a HIT attributable to the disabled rule. If it still does, keep the rollout stopped and use the provider's supported invalidation process while investigating the rule state.

If origin health regresses, also restore the prior non-secret pool/admission configuration through the service's normal change process and restart the single existing process once. Do not increase pool limits without measured capacity evidence and do not introduce additional workers. Verify origin health and approved GET status after restart. Record rollback times, status and cache headers, and redacted service health. Keep `/gift/sc` and mutation routes outside the cache throughout rollback.

## Completion record

Before closing the change, record:

- Confirmed Host-to-origin mappings and co-hosted path ownership, or explicitly state that topology remains unconfirmed and no rules were enabled.
- Application release identifier and confirmation that production remained single-process with one Uvicorn worker.
- Exact EdgeOne rules enabled, if any, including Host, exact path, method, TTL, query-key behavior, and 404/503 policy.
- Header-only status and cache-status results for each approved path, distinct query keys, and exclusion checks.
- Canary observation window, pool/admission health, rollback result if used, and remaining issues. Keep WebSocket symptoms separate from cache results.

No production console configuration, topology mapping, hit result, or successful rollout is asserted by this runbook. Those facts require operator evidence collected after authenticated access is available.
