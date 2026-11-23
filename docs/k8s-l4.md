# Running at scale on GKE with NVIDIA L4 (KServe + Triton)

The local docker-compose stack proves functionality and correctness; proving **10k+ TPS with
headroom** needs dedicated nodes per component. `deploy/k8s` ships a kustomize base plus two
overlays: `dev` (one node, mock model) and `gke-l4` (production-shaped, Triton on L4).

## 1. Cluster and node pools

```bash
gcloud container clusters create realy --region europe-west4 --release-channel regular \
  --num-nodes 1 --machine-type e2-standard-4
gcloud container node-pools create realy-kafka   --cluster realy --machine-type n2-standard-8   --num-nodes 1 --disk-type pd-ssd
gcloud container node-pools create realy-flink   --cluster realy --machine-type n2-highmem-8   --num-nodes 2 --local-nvme-ssd-block count=1
gcloud container node-pools create realy-serving --cluster realy --machine-type c3-standard-8  --num-nodes 2
gcloud container node-pools create realy-gpu     --cluster realy --machine-type g2-standard-8  --num-nodes 1 \
  --accelerator type=nvidia-l4,count=1,gpu-driver-version=latest
gcloud container node-pools create realy-driver  --cluster realy --machine-type c3-standard-8  --num-nodes 1
```
(`--num-nodes` is per zone for regional clusters.)

## 2. Operators

```bash
kubectl create ns realy
helm repo add strimzi https://strimzi.io/charts/ && helm install strimzi strimzi/strimzi-kafka-operator -n realy
helm repo add flink-operator https://downloads.apache.org/flink/flink-kubernetes-operator-1.13.0/
helm install flink-kubernetes-operator flink-operator/flink-kubernetes-operator -n realy
# KServe (RawDeployment mode, no Knative) + cert-manager: https://kserve.github.io/website/latest/admin/kubernetes_deployment/
helm install kube-prometheus-stack prometheus-community/kube-prometheus-stack -n monitoring --create-namespace
```

## 3. Images and models

```bash
make images                                   # gateway, mock-model, flink, console (all from source)
for i in scoring-gateway mock-model-server flink console; do
  docker tag realy/$i:dev REGISTRY/realy/$i:v0.2.0 && docker push REGISTRY/realy/$i:v0.2.0; done
pulsebench train --run runs/<a> --run runs/<b>     # -> models/triton/{fraud_mlp,fraud_gbdt}
gsutil -m cp -r models/triton gs://BUCKET/realy/models/
```
Replace `REGISTRY` and `REPLACE_BUCKET` in `deploy/k8s/overlays/gke-l4` and `components/operators/flink.yaml`. For
"structurally identical" proxies, train with the production tensor width and capacity
(`pulsebench train --input-dim 512 --hidden 512,256,128 --trees 800 --depth 8`): the gateway and Flink
read the model's input width from Triton's model metadata and zero-pad to it (`MODEL_INPUT_DIM`
overrides), so the GPU executes real GEMMs and host→device transfers of production size.

## 4. Deploy and run

```bash
python -m realy.k8s && make k8s-validate      # generated manifests + schema validation
kubectl apply -k deploy/k8s/overlays/gke-l4
kubectl -n realy get kafka,flinkdeployment,inferenceservice,pods
kubectl -n realy create job --from=job/bench-run step-ramp-1      # or edit args: run baseline-10k
kubectl -n realy port-forward svc/console 8090                     # operator console
```
The console pod (and the optional `bench-run` Job) runs emitters **and** collectors (one clock
domain); results land on the `realy-console` PVC and in ClickHouse (`realy.decisions`, `v_latency_by_second`, `v_training_dataset`).

## Notes

* Triton's gRPC endpoint: `fraud-models-predictor:9000` (RawDeployment); verify with
  `kubectl -n realy get svc` — KServe versions differ in the exposed port name.
* The mock model stays deployed: switching `MODEL_TARGET` back is the A/B for "is the model the
  bottleneck?".
* Flink topologies are FlinkDeployments managed by the operator (application mode, savepoint upgrade);
  set `FLINK_REST` empty for the driver so it does not try to manage jobs itself, and switch
  `realy-features` / `realy-inline` by editing `spec.job.state`.
* Not validated in this repository's CI: actual GPU execution (no L4 available) — the manifests are
  schema-validated, and the mock server speaks the identical KServe v2 protocol so the client path is
  exercised end to end.
