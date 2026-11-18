# 架构总览与方案选型

本文档阐述 ModelDock 的系统定位、方案选型依据、整体架构拓扑以及关键工程决策（D1–D10）。

---

## 1. 背景与为什么独立为单独 Operator

在同时包含交互式开发（`DevPilot`）、分布式训练/强化学习（`TrainPilot`）与分布式在线推理（`InferPilot`）的容器云平台中，模型资产如果直接附属在训练或推理控制面内部，会迅速遇到四个结构性矛盾：

1. **跨集群与安全域隔离**：训练集群（高带宽 RoCE/RDMA、高抢占率）与在线推理集群（高可用、公网 API 暴露）通常物理分离。若注册中心内嵌于 `InferPilot`，训练任务导出模型必须跨安全域调用推理控制面接口。
2. **多源异构输入**：推理服务消费的模型不仅来自内部 `TrainPilot` 产出的 Checkpoint，还包括平台同步的开源基座（HuggingFace / ModelScope）、`DevPilot` 交互式微调产物、离线量化（FP8 / AWQ）流水线产物以及租户自主上传的私有权重。
3. **100:1 的生命周期漏斗**：一次训练任务会高频落盘数十个中间 Checkpoint，但只有经过评测（Eval）验证的极少数版本会进入生产推理池。中间需要独立的状态门禁（`Staging` → `Verified` → `Production`）与不可变版本锚定。
4. **复用云原生基础设施红利**：平台已具备成熟的 **Harbor + 对象存储 + Dragonfly P2P** 镜像基础设施。若另起炉灶维护一套基于传统 MLflow 或自定义 HTTP 文件服务的模型中心，不仅重复建设鉴权、配额、跨机房同步与垃圾回收，也无法享受 OCI 分片并发拉取和 P2P 分发加速。

因此，ModelDock 定位为**基于 Harbor 构建的独立 Kubernetes Operator**，对上承接训练产物归档与外部导入，对下向推理引擎交付带有完整运行时契约（`ServingProfile`）的不可变模型版本。

---

## 2. 底座方案选型对比

| 维度 | 方案 A：传统 MLflow Model Registry + 裸 S3 路径 | 方案 B：Git LFS / 类 HuggingFace Hub 自建 | 方案 C（ModelDock 选型）：Harbor OCI Artifact (ModelPack) + K8s CRD |
| --- | --- | --- | --- |
| **内容不可变性** | 弱（S3 前缀下文件易被覆盖或部分缺失，无统一树级校验） | 强（Git Commit + LFS SHA256），但大仓库元数据重 | **强**（OCI Manifest `sha256` 锁定全部分片与配置层，天然防篡改） |
| **跨机房/跨集群分发** | 需自行开发 S3 跨区同步脚本与元数据双写逻辑 | 跨集群 Mirror 运维重，几十 GB 单文件易超时 | **原生内置**（直接复用 Harbor Registry Replication 策略与 Webhook） |
| **大规模并发拉取加速** | 100 个推理 Pod 同时拉 S3 易打满网关或触发限流 | HTTP 单流拉取，缺乏集群内节点间 P2P 加速 | **原生支持 Dragonfly / Kraken P2P**，按 OCI Blob 分片并发分发 |
| **LLM 专有契约表达** | 面向传统表格/小模型，缺乏 LoRA 基座绑定、KV/显存规格、ChatTemplate 校验 | 依赖自由格式 `README.md` YAML 头，缺乏强 Schema 约束 | **K8s CRD 强类型 Schema + ValidatingWebhook**，编译期/准入期拦截配置错误 |
| **多租户与凭证体系** | 需额外维护一套 IAM / STS 签发服务 | 需独立维护用户库与 Token 体系 | **Harbor Project + Robot Account** 自动映射 K8s Namespace，最小权限隔离 |
| **运维复杂度** | 需额外部署 PostgreSQL + MLflow Server 高可用集群 | 需维护 Git/LFS 存储集群与数据库 | **零额外有状态数据库**（元数据即 K8s CRD + OCI Config，存储复用已有 Harbor） |

---

## 3. 整体系统拓扑

```text
┌───────────────────────────────── 上游生产端 (Producer) ─────────────────────────────────┐
│  1. TrainPilot (TrainTask SFT / RL 里程碑保存)                                           │
│  2. DevPilot (DevBox 交互式调优 / 量化脚本产出)                                          │
│  3. 外部模型源同步 (HuggingFace / ModelScope / 租户私有 ORAS 推送)                       │
└───────────────────────────────┬─────────────────────────────────────────────────────────┘
                                │ 创建 ModelExport CR (声明源路径/源地址 + 目标 ModelRepository)
                                ▼
┌─────────────────────── ModelDock 控制面 (modeldock-system) ─────────────────────────────┐
│  ├─ ModelRepositoryReconciler ──▶ 调用 Harbor API 对账 Project、配额、GC 与 Robot Secret│
│  ├─ ModelExportReconciler     ──▶ 调度非特权 modeldock-exporter Job 流式校验并推送 OCI  │
│  ├─ ModelArtifactReconciler   ──▶ 同步 OCI Manifest/Config，维护阶段状态机与 LoRA 血缘  │
│  └─ ModelCacheReconciler      ──▶ 编排 CAS 共享卷解包任务或节点 DaemonSet P2P 预热       │
└───────────────┬───────────────────────────────────────────────┬─────────────────────────┘
                │ OCI v1.1 Push / Harbor Webhook                │ 编排预热与只读挂载视图
                ▼                                               ▼
┌───────────────────────────────────────┐     ┌───────────────────────────────────────────┐
│      Harbor 企业级制品中心            │     │       模型加速交付层 (ModelCache)         │
│  ├─ models-public (平台公共基座池)     │────▶│  ├─ 模式 1: 共享文件系统 CAS 池 (默认推荐)│
│  ├─ models-<tenant> (租户私有模型池)   │ P2P │     /models/sha256/<digest>/ (只读硬链接) │
│  └─ OCI ModelPack Manifest + Blobs    │ 拉取│  ├─ 模式 2: 节点本地 NVMe HostPath 缓存   │
└───────────────────────────────────────┘     │  └─ 模式 3: K8s 1.33+ 原生 OCI ImageVolume│
                                              └─────────────────────┬─────────────────────┘
                                                                    │ 零拷贝挂载 / 动态热加载
                                                                    ▼
┌───────────────────────────────── 下游消费端 (Consumer) ─────────────────────────────────┐
│  1. InferPilot (InferenceService 独占/公共推理池、LoRABinding 共享基座热挂载)            │
│  2. TrainPilot (下一阶段 SFT / RL Actor 初始权重挂载)                                    │
│  3. 自动化评测任务 (Eval Job) / 离线量化任务 (FP8 / AWQ Quantization Job)                │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. 关键设计决策（D1–D10）

### D1：不自建元数据关系型数据库，以 K8s CRD + OCI Config JSON 为双锚点
ModelDock 不引入独立的 MySQL/PostgreSQL 实例：
- **集群内实时状态与调度契约**存储在 `ModelRepository` 与 `ModelArtifact` CRD 中，天然支持 K8s RBAC、Informer Watch 与 GitOps。
- **跨集群可移植元数据**（模型架构、参数量、量化精度、基础模型 Digest、`TrainTask` 血缘摘要、`chat_template`）同步固化写入 Harbor 内部的 **OCI ModelPack Config Blob**。当 Harbor 通过 Replication 将模型同步到远端推理集群时，远端集群的 ModelDock 仅需拉取几 KB 的 OCI Manifest + Config 即可完整重建 `ModelArtifact` CR，无需跨集群同步数据库。

### D2：遵循 CNCF ModelPack / OCI v1.1 Artifact 标准，拒绝把模型打进 Docker 镜像
将几十 GB 模型塞入普通容器镜像会导致 `containerd` 解压耗时翻倍、占用节点 rootfs 空间，且推理引擎升级版本时需重推整个模型镜像。ModelDock 采用标准 OCI Artifact（`artifactType: application/vnd.cncf.model.manifest.v1+json`）：
- 每个 `model-00001-of-000xx.safetensors` 独立作为一个 OCI Blob Layer，多个模型版本若共享部分未修改分片可直接在 Harbor 内跨版本去重。
- 引擎容器镜像（如 `vllm-openai:v0.8.x`）与模型 OCI Artifact 完全解耦、独立演进。

### D3：高熵权重分片默认免压缩（Uncompressed Raw Blob）流式直传
BF16 / FP16 / FP8 浮点权重张量的信息熵极高，`gzip` 或 `zstd` 压缩率通常不足 3%～8%，却会在导出端消耗巨量 CPU 时间，并在拉取端造成解压瓶颈。
- 对 `.safetensors`、`.bin`、`.gguf` 等大体积权重层，`modeldock-exporter` 默认使用 **`application/vnd.cncf.model.weight.v1.raw`（无压缩、无 tar 包头封装的单文件直传）**，拉取落盘时可直接 `sendfile` / 并发区间写入目标文件，无需流式解压。
- 对 `tokenizer.json`、`config.json`、`chat_template.jinja` 等小文本配置打包为单个 `zstd` 压缩 tar 层，兼顾减少 HTTP 请求次数与元数据读取效率。

### D4：导出器（`modeldock-exporter`）零本地双倍落盘
训练任务在共享 PVC 上产出的 70B BF16 Checkpoint 高达 ~140 GB。若先在导出 Pod 本地打包成临时 tar 再上传，要求节点提供 140 GB 临时磁盘且耗时翻倍。`modeldock-exporter` 直接只读挂载训练共享 PVC：
1. 第一遍顺序流式计算或读取文件 SHA256 与字节长度；
2. 第二遍直接从原文件描述符分块（Chunked Upload）推送至 Harbor Blob 接口，本地临时磁盘占用 `< 64 MiB`。

### D5：内容寻址共享缓存池（CAS Shared Volume）作为主推交付路径
虽然 Kubernetes 正在演进原生 `ImageVolume`（KEP-4639），但在当前生产环境中，让几十个 GPU 节点各自通过 kubelet 拉取上百 GB OCI 制品仍会拖慢弹性扩容速度。ModelDock 主推 **`ModelCache` 共享存储 CAS 模式**：
- 由 `modeldock-puller` 将 Harbor 中的 OCI 模型一次性并发拉取到高性能共享文件系统（如 `AxonFS`、JuiceFS、CephFS、VAST NFS）的只读内容寻址目录：`<cache-root>/sha256/<manifest-digest>/`；
- 校验全部分片完成后原子写入 `.ready` 标记文件；
- 同一集群内无论扩容 1 个还是 64 个 `InferPilot` 推理副本，Pod 启动均以 `ReadOnlyMany` + `subPath: sha256/<manifest-digest>` 毫秒级直接挂载，实现**“一次预热、全集群零等待扩容”**。
- 同时保留 **节点本地 NVMe HostPath 缓存模式** 供无高性能共享文件系统的集群或超低延迟加载场景选用。

### D6：一等公民支持 `LoRAAdapter` 与基座强兼容校验
多租户微调与强化学习大量产出百 MB 级的 LoRA 适配器。ModelDock 在 CRD 层强制区分 `FullWeight` 与 `LoRAAdapter`：
- 当 `artifactType: LoRAAdapter` 时，必须声明 `baseModelRef`（锁定基座模型的名称与推荐 Digest）并自动解析 `adapter_config.json` 提取 `r`（Rank）、`lora_alpha`、`target_modules`；
- 下游 `InferPilot` 创建 `LoRABinding` 时，Admission Webhook 会直接比对目标共享基座 `InferenceService` 的基座模型与 `max_lora_rank`，在准入阶段拦截不兼容挂载，防止打挂线上共享 vLLM 实例。

### D7：严格划清强化学习（RL）协同的“慢路径”与“快路径”
- **慢路径（归属 ModelDock）**：RL 训练的初始 SFT 基座读取、训练过程中每隔数百步保存的 Milestone Checkpoint、以及训练结束后的最终 Policy 模型发布，均通过 `ModelExport` → `ModelArtifact` 纳管。
- **快路径（严禁经过 ModelDock）**：GRPO / PPO 在线采样（Rollout）过程中每一步（秒级）的 Actor → Rollout Engine 权重刷新，完全走 `TrainPilot` 与 `InferPilot` 之间的 RDMA / NCCL / NIXL 内存同步通道（见 `InferPilot` 的 `RolloutChannel` 设计），绝不写入 Harbor 或创建 `ModelArtifact`。

### D8：多租户 Namespace 映射与三级可见性控制
- 每个租户工作空间 Namespace（带 `workspace.wutz.dev/project: <tenant>` 标签）中的 `ModelRepository` 自动映射到 Harbor 中的 `models-<tenant>` 私有项目，Robot Account 凭证仅下发至该 Namespace。
- 平台管理员维护 `modeldock-public` Namespace（对应 Harbor `models-public` 项目），存放 `Qwen3`、`DeepSeek`、`Llama` 等公共基座。
- 支持三级可见性：`Private`（仅本 Namespace 可引用）、`WorkspaceShared`（显式白名单授权其他租户 Namespace 引用）、`Public`（全集群任意租户可读，支持直接作为租户 LoRA 的基座或一键拉起专属推理池）。

### D9：拒绝在仓库中保存 `pickle` / 任意代码执行隐患（安全默认项）
`modeldock-exporter` 默认开启格式安全策略：优先要求 `.safetensors` 权重格式；若历史模型包含 `.bin` / `.pt`（PyTorch Pickle），必须在 `ModelExport` 中显式声明 `allowUnsafeFormats: true` 并在生成的 `ModelArtifact` 状态打上 `UnsafeSerialization= true` 警示标签，同时对 `trust_remote_code` 需求显式标记，供 `InferPilot` 安全策略引擎审计。

### D10：跨集群事件驱动联动（Harbor Webhook → Operator）
除主动轮询外，ModelDock 控制器内置轻量级 Harbor Webhook 接收端点：当外部开发者使用 `oras` /ModelDock CLI 直接向 Harbor 推送合规模型，或主训练集群 Harbor 向推理集群 Harbor 完成跨站复制（Replication）时，Webhook 自动触发对应 `ModelRepository` 扫描并在集群内生成 `ModelArtifact`，必要时根据策略自动创建 `ModelCache` 提前完成冷启动预热。
