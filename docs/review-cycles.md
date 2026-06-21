# Review Cycles

The project was completed through five "finish, review, improve, test" cycles.

## Cycle 1: Version Baseline

Finished:

- Moved the lab to Kafka `4.1.2` and Flink `2.1.2`.
- Pinned the Flink Kafka connector to `4.0.1-2.0`.

Reviewed:

- Docker image availability.
- Maven artifact availability.
- Compose config consistency.

Improved:

- Added version rationale in `docs/version-decision.md`.

Tested:

- `docker compose config`
- Docker manifest checks
- Maven metadata checks

## Cycle 2: Streaming Core

Finished:

- Added replay topic consumption.
- Added merchant anomaly alerting.
- Added late event handling into DLQ.

Reviewed:

- Whether each scenario maps to a common production streaming concern.

Improved:

- Added `transactions.replay` and DLQ metadata fields.
- Added focused rule tests.

Tested:

- Rule unit tests for high-risk, burst, merchant anomaly, and replay eligibility.

## Cycle 3: Docker Operations

Finished:

- Added Makefile commands for lag, DLQ, replay, and smoke checks.
- Added replayer service.

Reviewed:

- Whether a new learner can run the system in about 10 minutes.

Improved:

- Expanded smoke checks to alerts, aggregates, and DLQ.

Tested:

- Compose rendering and Python compile checks.

## Cycle 4: Kubernetes

Finished:

- Added Strimzi Kafka and Flink Kubernetes Operator manifests.
- Added dev and prod-like overlays.

Reviewed:

- Whether the manifests are useful to engineers familiar with GitOps/operator workflows.

Improved:

- Separated local dev settings from prod-like replication/resource settings.

Tested:

- `kubectl kustomize k8s/overlays/dev`
- `kubectl kustomize k8s/overlays/prod-like`

## Cycle 5: Learning And Handoff

Finished:

- Reworked README, learning guide, schema docs, runbook, replay guide, Kubernetes guide, and CI.

Reviewed:

- Learning usefulness.
- Enterprise reference value.
- Interview and collaboration explainability.

Improved:

- Documented what is runnable locally and what must change for production.

Tested:

- Static checks, render checks, and CI workflow coverage.
