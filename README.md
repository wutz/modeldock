# ModelDock

面向训推一体平台的 Kubernetes 模型注册与 OCI 制品分发 Operator（API Group：`modeldock.wutz.dev/v1alpha1`）。

ModelDock 以 **Harbor（OCI v1.1 Artifact / CNCF ModelPack 规范）** 为不可变存储与 P2P 分发底座，作为连接上游训练/强化学习平台（[`TrainPilot`](https://github.com/wutz/trainpilot)）、交互式开发机（[`DevPilot`](https://github.com/wutz/devpilot)）与下游分布式推理平台（[`InferPilot`](https://github.com/wutz/inferpilot)）的**中立模型资产契约中心（Single Source of Truth）**。

> 状态：**M0 架构规划与 CRD 规范设计阶段**（当前仓库仅包含架构设计与接口契约文档，尚未进入编码阶段）。

## 解决的核心工程矛盾

1. **模型控制面与大体积权重数据面彻底解耦**：Operator 仅通过 Kubernetes CRD 管控模型元数据、训练血缘、评测门禁、推理运行时契约（`ServingProfile`）与租户权限；数十至数百 GB 的 `safetensors` 权重文件直接通过非特权流式构建器与 Harbor 对象存储后端直传直拉，绝不经过控制面转发。
2. **Harbor 原生 OCI 分片封装与零双倍落盘导出**：遵循 CNCF ModelPack 规范，将模型权重分片（`safetensors`）、分词器/对话模板（`tokenizer` / `chat_template`）与 LoRA 适配器配置拆分为独立 OCI Layer。`modeldock-exporter` 直接从训练共享盘 PVC 流式校验并按块直传 Harbor（对高熵浮点权重自动采用免压缩直传），无需在导出节点二次拷贝完整副本。
3. **跨集群训推解耦与秒级冷启动预热**：针对训练集群与在线推理集群物理隔离场景，借助 Harbor 跨站同步与 P2P 镜像分发（Dragonfly），配合 `ModelCache` 控制器将制品按 Digest 内容寻址预热至共享存储（如 `AxonFS` / JuiceFS / CephFS）或节点本地 NVMe，供下游 `InferPilot` 的 `vLLM` / `SGLang` 副本毫秒级只读挂载。
4. **厘清强化学习（RL）慢速归档与高速同步边界**：仅承接 SFT 产出、RL 初始基座、里程碑 Checkpoint 与正式发布策略版本（慢路径制品流）；明确旁路 RL 在线训练每一步的 Rollout 显存权重刷新（快路径交由 RDMA/NIXL 直传），避免元数据风暴污染版本库。

## 核心架构

```text
TrainPilot (TrainTask) / DevPilot (DevBox) / 外部源 (HF / ModelScope)
   │  提交 ModelExport：流式读取训练 PVC 或远端源，校验 safetensors 与 LoRA 契约
   ▼
┌─────────────────── 隔离导出任务 (modeldock-exporter Job) ───────────────────┐
│ 非特权 Pod (privileged: false, 零本地双倍落盘)                              │
│  ├─ 解析 config.json / tokenizer_config.json / adapter_config.json         │
│  ├─ 按 CNCF ModelPack 组装 OCI Config + 分片 Layer (safetensors 免压缩直传) │
│  └─ 使用短期 Robot Token 推送至 Harbor 并锁定不可变 Digest (sha256:...)      │
└─────────────────────────────────────┬───────────────────────────────────────┘
                                      │ OCI v1.1 Registry API + Webhook
                                      ▼
┌────────────────────── Harbor 模型底座 (OCI + 对象存储) ──────────────────────┐
│  ├─ 租户隔离 Project (models-<workspace>) & 公共基座库 (models-public)       │
│  ├─ Harbor Robot Account 最小权限读写凭证自动对账                            │
│  └─ 跨集群 Registry Replication (训练集群 Harbor ──▶ 在线推理集群 Harbor)     │
└─────────────────────────────────────┬───────────────────────────────────────┘
                                      │
          ┌───────────────────────────┴───────────────────────────┐
          ▼ 控制面契约 (ModelRepository / ModelArtifact)            ▼ 数据面预热 (ModelCache + Dragonfly P2P)
┌─────────────────────────────┐                     ┌─────────────────────────────────────────┐
│ 血缘溯源 (TASK_ID / RUN_ID)  │                     │ 内容寻址缓存池 (/models/sha256/<digest>) │
│ LoRA 基座强绑定校验          │                     │  ├─ RWX 共享文件系统一次解包多副本只读挂载│
│ 阶段晋级 (Staging→Prod)      │                     │  └─ 节点 NVMe HostPath 本地高速热缓存   │
└──────────────┬──────────────┘                     └────────────────────┬────────────────────┘
               └───────────────────────────┬─────────────────────────────┘
                                           ▼
                         InferPilot (InferenceService / LoRABinding)
                      (llm-d 分布式推理池秒级挂载 & 动态 Multi-LoRA 热加载)
```

## 核心特性与生态对齐

| 能力维度 | 设计机制 | 关联生态契约 |
| --- | --- | --- |
| Harbor 项目与凭证编排 | `ModelRepository` 自动对账 Harbor Project、配额、保留策略与最小权限 Robot Account Secret | Harbor v2.10+ REST API、`workspace.wutz.dev/project` |
| 标准 OCI 模型封装 | 遵循 CNCF ModelPack 规范：权重分片独立成层、配置层显式提取架构/精度/显存估算/ChatTemplate | OCI v1.1 Artifact、ORAS、`go-containerregistry` |
| 全量模型与 LoRA 双形态 | 区分 `FullWeight`、`LoRAAdapter`（强校验 `baseModelRef` 与 `loraRank`）及 `DraftModel` | 对接 `InferPilot` 独占推理池与共享基座动态 Multi-LoRA |
| 训推血缘与生命周期门禁 | 记录 `TrainPilot` `TASK_ID`/`RUN_ID`/Step 与评测分数，提供 `Staging` → `Verified` → `Production` 晋级状态机 | `trainpilot.wutz.dev/v1alpha1` (`TrainTask`) |
| 预热与免解压极速投递 | `ModelCache` 将 Harbor OCI 模型按 Digest 幂等展开至共享只读卷或节点 NVMe 池，消除多副本重复拉取 | `AxonFS` / JuiceFS / CephFS、Dragonfly P2P |
| 多租户三级可见性 | 支持 `Private`（租户私有）、`WorkspaceShared`（指定租户白名单共享）、`Public`（平台公共基座目录） | Kubernetes RBAC + ValidatingWebhook 跨 Namespace 引用校验 |

## 文档目录

- [架构总览与方案选型](docs/architecture.md) — 为什么基于 Harbor OCI 构建模型中心、整体拓扑、关键设计决策（D1–D10）与核心数据流
- [Operator 与 CRD 规范](docs/operator-and-crd.md) — `ModelRepository`、`ModelArtifact`、`ModelExport`、`ModelCache` 四大 CRD 字段定义、状态机与 Webhook 约束
- [Harbor OCI 封装与极速分发机制深潜](docs/harbor-oci-and-delivery.md) — ModelPack 分层结构、免压缩流式直传、LoRA 兼容性校验与 CAS 预热缓存池细节
- [训推协同与多租户安全边界](docs/training-and-inference-synergy.md) — 对接 `TrainPilot` Checkpoint 晋级、RL 快慢路径切分、联动 `InferPilot` 灰度发布与 Robot 凭证隔离
- [演进路线图](docs/roadmap.md) — M0–M4 里程碑划分与端到端验收标准

## 规划交付物（后续编码阶段）

项目进入编码阶段后，将采用 Kubebuilder v4 构建并发布以下多架构组件与 Chart：

- `ghcr.io/wutz/modeldock` — Kubernetes Operator 控制器管理器与 Admission Webhook
- `ghcr.io/wutz/modeldock-exporter` — 流式校验并打包推送模型至 Harbor 的非特权执行器镜像
- `ghcr.io/wutz/modeldock-puller` — 支持分片并发与 P2P 加速的 CAS 缓存预热拉取器镜像
- `oci://ghcr.io/wutz/charts/modeldock` — 部署 Helm Chart

## 许可

Apache License 2.0
