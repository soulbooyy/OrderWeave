# OrderWeave 真实 DeepSeek 端到端 Smoke Test

2026-11-21，香港时间。3 个独立会话、4 个用户轮次均已执行，各案例只运行一次。**2 个案例通过，1 个案例失败。** 6 次 MCP 只读调用都取得与业务库一致的原始回包。唯一的失败来自三节点 Planner 漏掉两条顺序依赖，应用仍将该轮标为 `completed`。

## 代码与运行配置

原分支 `codex/retail-routing-ablation`，HEAD `5fa3f33ce7206bb355c4b0d672564428782439c1`。基线包括未提交的 Phase 3 和 Phase 4，实施文档已分别记录全量 498 和 537 项通过。开始前 23 个已跟踪文件有改动，3 个文件未跟踪。`git diff HEAD --binary --no-ext-diff` 输出的 SHA-256 为 `72f7cc99defb8613a3d8096bbb6810aae76b6d5bec67545e1ca5b3d7ce42b567`。此哈希不含未跟踪文件。

| 未跟踪文件 | SHA-256 |
|---|---|
| `core/agent_context_budget.py`（本次运行加载） | `14521e90de5b5354b58114c73cd0a40a7bcdb1c75f75de1ebb78d9e480e3beae` |
| `tests/test_agent_context_budget.py` | `50b6784c4aa0784b4f74f8791c776ef98f254e23e2248c065baffefb313c615f` |
| `tests/test_dag_context_recovery.py` | `54ee718869c82c4fb5b2a63ddf42d65996b20a48e93fa0be3589701eb9bf251c` |

通过正常 `RetailService.handle_turn` 进入 multi / DAG / LangGraph / compiled，连接真实 stdio MCP 子进程。启用会话上下文，窗口为最近 4 个完整外部轮次，保留已有 SQLite 全局摘要机制。本次最长会话 2 轮，实际摘要调用为 0。工具日志为 J，模型窗口保护为 observe。为沿用英文冻结输入，运行期使用英文；模型思考沿用已配置的 disabled。其他业务设置沿用项目配置。各会话分别使用新的业务库和 checkpoint，外部 Memory 与用户画像关闭，预留独立命名空间。通过公开种子的邮箱在各自测试库预绑定合法所有者。种子 SHA-256 为 `413a65160adbdb5fde0ffc0015c49b6d70250b10c18128de169b597af7766765`。

模型目录鉴权 HTTP 200。Anthropic 兼容端点的全部 21 个生成响应均返回 `model=deepseek-flash`。供应商当日公布的别名版本为 `DeepSeek-V4.1-Flash-1017`，响应只披露别名。三个会话共享一个新建的 3 元 SQLite 账本，每轮最多 12 次，全程最多 36 次。预留按峰时未命中输入 2、输出 8 元/百万 Token 计算；周六结算按未命中输入 1、命中输入 0.02、输出 4 元/百万 Token 计算。SDK 隐式重试为 0，未使用模型 Judge。[DeepSeek 官方模型与价格](https://api-docs.deepseek.com/zh-cn/quick_start/pricing/)

## 逐案例核验

下文以订单尾号和商品尾号标识种子实体。逐字输入、原始 Planner JSON、每次验证结果与最终文本保存在隔离证据目录中。

| 案例与输入 | 实际执行与核验 | 判定 |
|---|---|---|
| C1，要求统计订单尾号 9057 的商品数 | Planner 首次通过，`order_lookup/purchased_item_count` 派给 General。一次 `get_order_details` 返回归属正确的 processed 订单。原始订单、SQLite 和种子逐项一致，Composer 回复 2 件。 | **通过** |
| C2，冻结的 `repeat-specialist` 原文，依次查询款式 1831 的颜色、订单 8778 的件数、款式 9236 的尺码 | Planner 正确给出三个类型和目标，源码默认 owner 实际依次为 Technical、General、Technical。MCP 依次调用 `get_item_details`、`get_order_details`、`get_item_details`。最终回复依次包含 black、5 件、full size，与数据库、冻结答案一致。 | **失败，规划漏边** |
| C3，第 1 轮要求统计订单 4160 的商品数；第 2 轮原文为 “Check the status of that order.” | 首轮 General 查得 2 件。第二轮出站 Planner 请求实际包含首轮用户输入和已发布回复。首次输出把历史编号放入字面 `target`，校验报 `unverified_target`。同轮内置纠错改为 `target=""` 加 `history_target` 后通过。宿主核验来源轮次 1、绑定及订单归属，General 再查同一订单，最终回复 delivered，与业务库一致。 | **通过，发生一次正常规划纠错** |

**C2 的失败归因。** 冻结依赖为 `a → b → c`。原始规划将三项解释为独立查询，实际持久化的三个节点依赖列表均为空。结构校验首次通过，调度顺序碰巧与用户原文一致，但发给 b、c 的模型请求中 `inputs` 和 `input_refs` 均为空。此例暴露了规划错误以及现有校验对缺边的容许；不能据此判定调度器违反了收到的依赖。C3 的首次失败来自历史目标字段使用错误，首轮历史已到达模型。所有四轮的 Composer 文本与最终发布文本相同，Response Guard 均为 `checked`，无改写、无 findings。

## 请求与费用

输入总量包含缓存读取；所有请求的缓存写入均为 0。费用按真实响应 usage、账本费率和请求时段核算，未取得独立的供应商扣款明细。延迟为用户轮次服务调用时长，不含建库和启动 MCP。

| 会话/轮 | 模型请求 | 输入 Token | 其中缓存命中 | 输出 Token | 账本费用（元） | 延迟（秒） |
|---|---:|---:|---:|---:|---:|---:|
| C1 / 1 | 4 | 8,444 | 2,560 | 215 | 0.00679520 | 3.205 |
| C2 / 1 | 8 | 18,856 | 14,336 | 509 | 0.00684272 | 7.004 |
| C3 / 1 | 4 | 8,432 | 7,296 | 213 | 0.00213392 | 3.058 |
| C3 / 2 | 5 | 11,376 | 9,984 | 345 | 0.00297168 | 4.780 |
| **合计** | **21** | **47,108** | **34,176** | **1,282** | **0.01874352** | **18.047** |

账本阶段分布为 `plan` 请求清单 4 次、`intent` DAG 规划及其纠错 5 次、角色模型 12 次。请求全部成功结算，usage 缺失、预留越界、未结算请求均为 0。

## 安全检查、证据与 Phase 5 判断

核对了 6 次真实工具调用的参数、角色工具白名单、J 原始回包、配对 ID 和成功 Run 的证据引用。三个测试库的 orders、users、products 全表摘要在各轮前后一致，待确认记录和业务操作记录均为 0，每个会话仅产生一次测试身份绑定事件。样本中未发现越权、交易写入或重复提交。三个场景均为只读，不能据此评价真实写入或崩溃恢复的成功率。

隔离证据位于原工作区 `data/evaluation/context-smoke/20261121-10qI4V5M/`（Git 忽略，仅本机留存）。根目录的 `ledger.sqlite` 保留逐请求 usage、预留和结算；各 `C*/turnN.json` 保留模型回包标识、实际 Planner 输入视图、原始规划、校验、任务分派、Composer 与 Guard 结果。各案例的 `business.sqlite` 保留原始 J、执行图和业务状态，`checkpoint.sqlite` 保留图断点。原始工具数据只在受限证据目录保存。

**Phase 5 判断。** 实际模型、MCP、账本和证据链已具备开展正式测量的条件。三节点依赖保持这一功能门槛本次未通过，正式评分需独立比较冻结依赖与实际边，并核验后继收到的证据。不能用工具调用次序、最终事实正确或 API 的 `completed` 代替这一项。本次未开展正式消融，停在人工审查点。
