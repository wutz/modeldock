# Operator 与 CRD 规范

API Group：`modeldock.wutz.dev/v1alpha1`（遵循 `*.wutz.dev` 家族统一约定，采用 Kubebuilder v4 脚手架标准设计）。

ModelDock 围绕模型资产的“建仓、导出、版本契约、缓存预热”四个正交环节，定义四个 Namespaced CRD：

1. **`ModelRepository`**（简称 `mrepo`）：模型家族仓库声明，对账 Harbor Project 仓库、访问可见性、保留策略与最小权限 Robot 凭证。
2. **`ModelArtifact`**（简称 `mart`）：不可变模型版本契约（锚定单个 Harbor OCI Manifest Digest），承载推理规格 `ServingProfile`、LoRA 基座绑定、训练血缘与晋级状态机。
3. **`ModelExport`**（简称 `mexp`）：一次性不可变导出任务，将训练 PVC 目录、开发机目录或外部源流式封装为 OCI ModelPack 并推送至 Harbor，完成后自动产出 `ModelArtifact`。
4. **`ModelCache`**（简称 `mcache`）：模型预热与本地/共享卷交付控制器，将指定 `ModelArtifact` 按 Digest 解包预热至共享存储或 GPU 节点 NVMe 池，向上游评测或下游 `InferPilot` 暴露零等待挂载路径。

---

## 1. `ModelRepository` CRD 定义

### 1.1 示例清单与字段结构

```yaml
apiVersion: modeldock.wutz.dev/v1alpha1
kind: ModelRepository
metadata:
  name: finance-qwen3-lora
  namespace: forge-acme
  labels:
    workspace.wutz.dev/project: acme
spec:
  displayName: "金融投研助手 Qwen3-32B LoRA 模型库"
  description: "基于平台公共 Qwen3-32B 基座经 SFT 与 GRPO 强化学习微调产出的适配器仓库"
  category: LLM                          # LLM | VLM | Embedding | Reranker | Audio | Custom

  # 可见性与跨租户授权
  visibility: Private                    # Private | WorkspaceShared | Public
  sharedNamespaces: []                   # 当 visibility=WorkspaceShared 时允许跨 Namespace 引用的白名单

  # Harbor 映射配置（留空则使用平台全局 Harbor 实例，Project 自动推导为 models-<project>）
  harbor:
    instanceRef: default-harbor          # 引用平台级 Harbor 连接配置
    projectOverride: ""                  # 仅平台管理员在 modeldock-public 下可指定 models-public
    repositoryName: finance-qwen3-lora   # 完整 OCI 引用为: harbor.example.com/models-acme/finance-qwen3-lora

  # 自动预热与生命周期回收策略
  autoWarmupStages:
    - Production                         # 当版本晋升至 Production 时自动创建/更新 ModelCache
  retentionPolicy:
    keepLastStagingVersions: 5           # Staging 阶段最多保留最近 5 个版本（Verified/Production 永不自动清理）
    untaggedGarbageCollectDays: 3
status:
  phase: Ready                           # Pending | Ready | Error
  ociRepository: harbor.example.com/models-acme/finance-qwen3-lora
  harborProjectID: 42
  pullSecretName: mrepo-finance-qwen3-lora-pull   # 自动签发的只读 Robot Secret
  pushSecretName: mrepo-finance-qwen3-lora-push   # 自动签发的读写 Robot Secret（供 ModelExport 使用）
  artifactCount: 4
  latestProductionArtifact: finance-qwen3-lora-v1-2-0
  conditions:
    - type: HarborSynced
      status: "True"
      reason: Reconciled
```

---

## 2. `ModelArtifact` CRD 定义

`ModelArtifact` 是连接 `ModelDock` 与 `InferPilot` / `TrainPilot` 的核心契约对象。一经锁定 `spec.ociDigest`，其指向的模型权重与分片列表即完全不可变；仅允许流转 `spec.stage`（阶段晋级）与补充评测记录。

### 2.1 示例清单（含 LoRA 与全量模型字段）

```yaml
apiVersion: modeldock.wutz.dev/v1alpha1
kind: ModelArtifact
metadata:
  name: finance-qwen3-lora-r32-step1200
  namespace: forge-acme
  labels:
    modeldock.wutz.dev/repository: finance-qwen3-lora
    modeldock.wutz.dev/artifact-type: LoRAAdapter
    modeldock.wutz.dev/stage: Production
spec:
  repositoryRef: finance-qwen3-lora
  versionTag: v1.2.0                     # 语义化版本标签（映射到 Harbor Tag）
  ociDigest: "sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
  stage: Production                      # Staging | Verified | Production | Deprecated | Archived

  # 制品形态：FullWeight | LoRAAdapter | DraftModel
  artifactType: LoRAAdapter

  # 当 artifactType=LoRAAdapter 或 DraftModel 时必填，用于 InferPilot 准入期强校验
  baseModelRef:
    namespace: modeldock-public
    name: qwen3-32b-instruct-bf16
    ociDigest: "sha256:4b227777d4dd1fc61c6f884f48641d02b4d121d3fd328cb08b5531fcacdabf8a"

  loraSpec:
    rank: 32
    alpha: 64
    targetModules: ["q_proj", "k_proj", "v_proj", "o_proj", "gate_proj", "up_proj", "down_proj"]

  # 推理运行时契约（由 modeldock-exporter 解析模型配置自动填充，支持人工微调覆盖）
  servingProfile:
    architecture: Qwen3ForCausalLM
    parameterSize: "32B"                 # 基座或自身参数规模
    precision: BF16                      # FP32 | BF16 | FP16 | FP8 | INT8 | INT4-AWQ | INT4-GPTQ
    serializationFormat: safetensors     # safetensors | gguf | pytorch-bin
    requiresTrustRemoteCode: false
    minVRAMGiB: 1                        # LoRA 额外显存开销估算（FullWeight 32B BF16 则约为 68 GiB）
    maxPositionEmbeddings: 32768
    hasChatTemplate: true
    recommendedEngine: vllm              # vllm | sglang | triton
    recommendedTensorParallel: 2
    recommendedExtraArgs:
      - "--enable-prefix-caching"

  # 训练与评测血缘溯源
  lineage:
    sourceType: TrainTask                # TrainTask | DevBox | HuggingFace | ModelScope | Quantization | ExternalOCI
    trainTaskRef:
      namespace: forge-acme
      name: grpo-finance-run04
      taskID: task-98a7c1
      runID: run-20261118-01
      globalStep: 1200
    parentArtifactRef:
      namespace: forge-acme
      name: finance-qwen3-sft-v1
    evaluations:
      - benchmark: "finance-eval-v2"
        score: "86.4"
        evaluatedAt: "2026-11-18T09:30:00Z"
        reportURI: "s3://eval-reports/acme/grpo-finance-run04-step1200.json"
status:
  phase: Ready                           # Syncing | Ready | Invalid | Archived
  ociReference: "harbor.example.com/models-acme/finance-qwen3-lora@sha256:9f86d081..."
  totalSizeBytes: 536870912              # 512 MiB
  shardCount: 1
  unsafeSerialization: false
  activeCacheRef: mcache-finance-qwen3-lora-9f86d081   # 若已完成预热，指向就绪的 ModelCache
  promotedToProductionAt: "2026-11-18T10:00:00Z"
  conditions:
    - type: ManifestVerified
      status: "True"
      reason: DigestMatched
    - type: BaseModelCompatible
      status: "True"
      reason: VerifiedAgainstBase
```

### 2.2 阶段晋级状态机（`spec.stage`）

```text
  [ModelExport 完成]
         │
         ▼
     ┌─────────┐   评测任务通过 / 人工确认   ┌──────────┐   发布上线审批   ┌────────────┐
     │ Staging │ ─────────────────────────▶ │ Verified │ ───────────────▶ │ Production │
     └────┬────┘                            └─────┬────┘                  └──────┬─────┘
          │ 自动保留策略淘汰                       │                              │ 新版本替代下线
          ▼                                       ▼                              ▼
     ┌──────────┐                                                          ┌────────────┐
     │ Archived │ ◀─────────────────────────────────────────────────────── │ Deprecated │
     └──────────┘                                                          └────────────┘
```

- **Webhook 保护不变量**：
  - 处于 `Production` 阶段且正在被活跃的 `InferenceService` 或 `LoRABinding` 引用的 `ModelArtifact`，Webhook 拒绝删除（必须先切换下游流量并降级为 `Deprecated`）。
  - 一旦进入 `Ready` 状态，`spec.ociDigest`、`spec.artifactType`、`spec.baseModelRef` 均设为不可变（Immutable）。

---

## 3. `ModelExport` CRD 定义

`ModelExport` 负责将文件系统目录（训练 PVC 或开发机快照）或外部仓库模型转化为标准的 Harbor OCI ModelPack 制品并自动注册 `ModelArtifact`。

```yaml
apiVersion: modeldock.wutz.dev/v1alpha1
kind: ModelExport
metadata:
  name: export-grpo-finance-step1200
  namespace: forge-acme
spec:
  targetRepository: finance-qwen3-lora
  versionTag: v1.2.0
  initialStage: Staging                  # 导出成功后自动创建的 ModelArtifact 初始阶段
  artifactType: LoRAAdapter

  baseModelRef:
    namespace: modeldock-public
    name: qwen3-32b-instruct-bf16

  # 数据源（三选一：pvcSource / externalHubSource / devBoxSource）
  source:
    pvcSource:
      claimName: project-shared-nvme     # 训练任务挂载的共享 PVC（导出 Job 以 ReadOnly 挂载）
      subPath: checkpoints/task-98a7c1/run-20261118-01/global_step_1200/actor/lora_adapter
      excludePatterns:
        - "optimizer.pt"                 # 默认自动剔除优化器状态与随机数种子，仅导出推理所需文件
        - "rng_state_*.pth"
        - "wandb/**"

  # 训练血缘注入（可由 TrainPilot 钩子或训练末尾脚本自动填写）
  lineage:
    sourceType: TrainTask
    trainTaskRef:
      namespace: forge-acme
      name: grpo-finance-run04
      taskID: task-98a7c1
      runID: run-20261118-01
      globalStep: 1200

  # 安全与性能选项
  allowUnsafeFormats: false              # 默认禁止导出包含任意代码执行风险的 .bin/.pt Pickle 权重
  concurrency: 4                         # 分片并发上传流数
  ttlSecondsAfterFinished: 86400         # 导出记录保留时长
status:
  phase: Succeeded                       # Pending | Validating | Pushing | Succeeded | Failed
  startTime: "2026-11-18T09:10:02Z"
  completionTime: "2026-11-18T09:10:18Z"
  exportedDigest: "sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
  artifactName: finance-qwen3-lora-r32-step1200
  bytesTransferred: 536870912
```

---

## 4. `ModelCache` CRD 定义

`ModelCache` 将 Harbor 中的 OCI 模型提前拉取解包到推理集群的缓存介质，解决几十至上百 GB 大模型扩容冷启动慢的核心痛点。

```yaml
apiVersion: modeldock.wutz.dev/v1alpha1
kind: ModelCache
metadata:
  name: mcache-qwen3-32b-bf16
  namespace: modeldock-public
spec:
  artifactRef:
    namespace: modeldock-public
    name: qwen3-32b-instruct-bf16

  # 缓存策略：SharedFilesystem (推荐，共享 RWX CAS 卷一次解包全集群秒挂) | NodeLocalNVMe (DaemonSet 预拉取至指定 GPU 节点组)
  strategy: SharedFilesystem

  sharedFilesystem:
    cachePoolName: default-rwx-model-pool # 平台级 RWX 存储池（如 AxonFS / JuiceFS / CephFS）

  nodeLocalNVMe:
    nodeSelector:
      scclabs.dev/gpu: H800-80G          # 仅当 strategy=NodeLocalNVMe 时生效，定向预热至目标推理机型节点
    hostCacheRoot: /var/lib/modeldock/cas

  enableP2P: true                        # 通过 Dragonfly P2P Proxy 加速拉取 Harbor OCI Blob
  ttlHoursAfterUnused: 168               # 无任何活跃 InferenceService 引用满 7 天后允许 LRU 回收缓存空间
status:
  phase: Ready                           # Pending | WarmingUp | Ready | Evicted | Failed
  resolvedDigest: "sha256:4b227777d4dd1fc61c6f884f48641d02b4d121d3fd328cb08b5531fcacdabf8a"
  mountSpec:
    mode: ReadOnlyManyPVC
    claimName: modeldock-cas-public-ro
    subPath: "sha256/4b227777d4dd1fc61c6f884f48641d02b4d121d3fd328cb08b5531fcacdabf8a"
  cachedNodesCount: 0                    # NodeLocalNVMe 模式下已预热就绪的 GPU 节点数
  warmupDurationSeconds: 26
  activeConsumers: 3                     # 当前正在挂载此缓存的 InferPilot 工作负载数
```

---

## 5. 快捷运维注解与准入控制（Webhook）汇总

| 目标资源 | 注解 / 校验项 | 行为说明 |
| --- | --- | --- |
| `ModelArtifact` | `modeldock.wutz.dev/action=warmup` | 立即触发创建关联的 `ModelCache` 进行缓存预热 |
| `ModelRepository` | `modeldock.wutz.dev/action=rotate-credentials` | 轮换 Harbor Robot Account Token 并滚动更新 Namespace 内的 Pull/Push Secret |
| `ModelArtifact` | 跨租户 `baseModelRef` 准入校验 | Webhook 校验目标 `baseModelRef` 所在 `ModelRepository` 的 `visibility` 是否对当前 Namespace 开放 |
| `ModelExport` | 训练器冗余文件过滤 | 默认拒绝将 `optimizer.pt`、`zero_pp_rank_*` 等百 GB 级训练优化器状态打入推理制品（除非显式声明 `includeTrainingStates: true`） |
