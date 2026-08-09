# prod-like overlay 사용 전 확인

이 overlay는 단일 노드 개발 구성을 다중 broker, persistent volume, savepoint 기반
업그레이드 구성으로 확장할 때 필요한 차이를 보여 주는 **검토용 예시**입니다. 그대로
배포할 수 있는 production manifest가 아닙니다.

적용 전에 최소한 다음 항목을 환경에 맞게 완성해야 합니다.

- `patch-flink-prod-like.yaml`의 `s3://replace-me-...` 경로 교체
- Flink image에는 S3 filesystem plugin이 포함되어 있습니다. Workload Identity, IRSA 또는
  secret 기반 인증 중 cluster 표준 방식을 연결하고 bucket 권한을 최소화
- Kafka listener TLS 및 사용자 인증, NetworkPolicy, secret 연동
- 실제 registry image 주소와 immutable tag 또는 digest 지정
- 조직의 resource quota, PodDisruptionBudget, topology spread 정책 반영
- Prometheus/Flink metric reporter, alert routing, SLO 기준 연결

값을 교체하기 전에는 `kubectl apply`가 아니라 아래 명령으로 렌더링 결과만
검토하십시오.

FlinkDeployment는 Kubernetes HA와 standby JobManager 2개를 사용합니다. Operator가 cluster ID를
CR 이름에서 관리하므로 `kubernetes.cluster-id`를 직접 추가하지 않습니다. HA metadata와
checkpoint가 같은 임시 bucket이 아니라 수명 주기가 관리되는 object storage에 남는지 확인합니다.

```bash
kubectl kustomize k8s/overlays/prod-like
```
