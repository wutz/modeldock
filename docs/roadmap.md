# 演进路线图

每个里程碑均以「自动化单测 / `envtest` 覆盖 + 在真实 Harbor 与 GPU 开发集群端到端验证通过」作为关闭条件。

---

## M0 架构规划与 CRD 契约设计（当前阶段）

- [x] 确立「独立模型注册 Operator + Harbor OCI v1.1 (CNCF ModelPack) 存储底座」技术路线
- [x] 完成控制面与数据面解耦设计：K8s CRD + OCI Config 双锚点无状态元数据模型
- [x] 定义四大核心 CRD（`ModelRepository`、`ModelArtifact`、`ModelExport`、`ModelCache`）字段与状态机规范
- [x] 划清与 `TrainPilot`（SFT 归档 / RL 慢路径）及 `InferPilot`（CAS 共享预热挂载 / LoRA 动态热加载 / RL 快路径旁路）的接口边界
- [ ] 完成跨项目（`TrainPilot` ↔ `ModelDock` ↔ `InferPilot`）联合评审，冻结 `v1alpha1` Go API 类型定义

---

## M1 Operator 脚手架、Harbor 项目对账与 `ModelArtifact` 生命周期

目标：跑通「创建 `ModelRepository` → 自动在 Harbor 创建隔离项目并签发最小权限 Robot Secret → 同步外部已推送的 OCI 模型生成 `ModelArtifact` → 阶段晋级与 Webhook 保护」闭环。

- [ ] 使用 Kubebuilder v4 初始化 Go 工程（`github.com/wutz/modeldock`，API Group `modeldock.wutz.dev/v1alpha1`）与 Helm Chart（`charts/modeldock`）
- [ ] 实现 Harbor v2 REST 客户端封装：Project 幂等创建、存储配额设置、Project Robot Account 签发与自动轮换
- [ ] 实现 `ModelRepositoryReconciler` 与 `ModelArtifactReconciler`：
  - [ ] OCI Manifest / ModelPack Config 远端元数据读取与 `ociDigest` 完整性校验
  - [ ] `Staging` → `Verified` → `Production` → `Deprecated` / `Archived` 状态流转与 Harbor 标签同步
  - [ ] ValidatingWebhook：锁定不可变字段、拦截删除正在被下游消费的 `Production` 版本、跨 Namespace 可见性校验
- [ ] 实现内置 Harbor Webhook 接收器，支持 `oras` / CLI 外部推送自动发现注册

---

## M2 `modeldock-exporter` 流式导出与 `TrainPilot` 训推打通

目标：跑通「`TrainPilot` 训练任务产出全量或 LoRA Checkpoint → 提交 `ModelExport` → 零本地双倍落盘流式直传 Harbor → 自动产出带完整 `TASK_ID`/`RUN_ID` 血缘与 `ServingProfile` 的 `ModelArtifact`」。

- [ ] 实现 `cmd/modeldock-exporter`（`CGO_ENABLED=0` 非特权静态二进制）：
  - [ ] `config.json`、`tokenizer_config.json`、`adapter_config.json` 自动解析器，自动推断模型架构、参数量、精度、LoRA Rank 与最小显存估算
  - [ ] 安全扫描器：路径穿越防御、`optimizer.pt` 训练状态自动过滤、`.safetensors` Header 校验与不安全 Pickle 格式拦截
  - [ ] 基于 `go-containerregistry` 的 ModelPack 组装器：小配置打包为 `tar+zstd` 层，大体积 `safetensors` 分片采用 `v1.raw` 免压缩并发流式直传与已存在 Blob 秒级跳过
- [ ] 实现 `ModelExportReconciler`：管理导出 Job 生命周期、失败重试、进度字节统计与 `ModelArtifact` 自动创建
- [ ] 与 `TrainPilot` 联调验证：分别测试 7B/32B 全量权重导出与 LoRA 适配器（百 MB 级）秒级导出

---

## M3 `ModelCache` 预热缓存池与 `InferPilot` 秒级挂载交付

目标：跑通「模型晋级 `Production` → 自动触发 `ModelCache` 并发拉取至共享 RWX CAS 存储池或节点本地 NVMe（经 Dragonfly P2P 加速）→ `InferPilot` 多副本扩容零等待直接挂载启动」。

- [ ] 实现 `cmd/modeldock-puller`：支持多协程分片并发下载、Dragonfly HTTP Proxy 对接、SHA256 流式校验与临时目录原子 `rename`
- [ ] 实现 `ModelCacheReconciler`：
  - [ ] `SharedFilesystem` 模式：CAS 内容寻址目录管理、跨 Namespace 只读 `subPath` PVC 暴露与引用计数追踪
  - [ ] `NodeLocalNVMe` 模式：按 GPU 机型标签定向预热、跨版本相同 Blob 硬链接去重拼装
  - [ ] 闲置缓存 TTL / LRU 自动垃圾回收（确保活跃 `activeConsumers > 0` 时绝不误删）

---

## M4 跨集群复制联动、量化评测钩子与内置只读控制台

目标：补齐多集群生产分发闭环与可视化资产目录。

- [ ] 验证跨集群 Harbor Replication（训练集群 → 在线推理集群）基于 `stage-production` 标签过滤同步与推理侧 `ModelArtifact` 自动重建
- [ ] 提供标准化 Post-Export 评测/量化触发契约（如自动派生 FP8 量化任务并将产物关联至 `parentArtifactRef`）
- [ ] 参照 `DevPilot` / `TrainPilot` 同进程 `go:embed` 模式提供轻量只读模型广场看板（透传调用者自身 K8s Bearer Token 遵守 RBAC）
