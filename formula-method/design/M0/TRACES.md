# PAT 实际路径摘录

以下原始路径全部来自本轮 PAT 结果，保留时间/线程无操作调度事件。事件个数不能用来估计真实时间。

## H01 — 存在从 send 到应用成功的完整路径。

结果：VALID；[原始日志](results/Healthy.txt)。

```text
<init -> c_begin -> b_accept -> b_dispatch -> b_io_begin -> b_io_complete -> cc_replica2_caught_up -> cc_replica1_caught_up -> cc_high_watermark -> time_broker -> b_produce_ok -> b_deliver -> c_poll>
```

## H02 — ISR=3 时仅一个远端追平尚不足以推进 HW。

结果：VALID；[原始日志](results/Healthy.txt)。

```text
<init -> c_begin -> time_client -> b_accept -> c_poll -> b_dispatch -> b_io_begin -> b_io_complete -> cc_replica1_caught_up>
```

## H06 — 调度负对照：不假设公平性时可永远只调度探针；此失败不是源码缺陷。

结果：NOT valid；[原始日志](results/Healthy.txt)。

```text
<init -> c_begin -> time_client -> b_accept -> b_dispatch -> b_io_begin -> b_io_complete -> cc_replica2_caught_up -> cc_replica1_caught_up -> cc_high_watermark -> time_broker -> b_produce_ok -> b_deliver -> time_broker -> (time_broker)*>
```

## P02 — 进程退出可以由 Kubelet 观察并重启。

结果：VALID；[原始日志](results/WholeProgress.txt)。

```text
<init -> fe_process_exit -> c_begin -> time_client -> c_poll -> k_restart>
```

## B01 — Controller TCP 已开放、Broker 处理尚未启用时，Ready 可先成立。

结果：VALID；[原始日志](results/Startup.txt)。

```text
<init -> b_process_start -> b_controller_bind -> b_client_bind -> b_metadata_load -> b_unfence -> k_startup_sample -> lp_sample -> rp_sample -> k_readiness_commit>
```

## B02 — 候选健康契约：Ready 应蕴含 Broker 处理启用；无时间模型给出反例。

结果：NOT valid；[原始日志](results/Startup.txt)。

```text
<init -> time_broker -> time_client -> c_poll -> b_process_start -> time_broker -> time_client -> c_poll -> b_controller_bind -> time_broker -> time_client -> c_poll -> b_client_bind -> time_broker -> time_client -> c_poll -> b_metadata_load -> time_broker -> time_client -> c_poll -> b_unfence -> time_broker -> time_client -> c_poll -> k_startup_sample -> time_broker -> time_client -> c_poll -> lp_sample -> time_broker -> time_client -> c_poll -> k_liveness_commit -> rp_sample -> time_broker -> time_client -> c_poll -> k_readiness_commit>
```

## N01 — 新 RP/LP 均至少成功两次，客户端仍可因独立链路故障交付超时。

结果：VALID；[原始日志](results/ClientNetwork.txt)。

```text
<init -> fe_client_cut -> c_begin -> time_client -> c_poll -> lp_sample -> k_liveness_commit -> lp_sample -> k_liveness_commit -> rp_sample -> k_readiness_commit -> lp_sample -> rp_sample>
```

## N02 — 先观察已提交、调用尚未结束且链路中断，随后客户端才交付超时。

结果：VALID；[原始日志](results/ClientNetwork.txt)。

```text
<init -> c_begin -> b_accept -> fe_client_cut -> b_dispatch -> b_io_begin -> b_io_complete -> cc_replica2_caught_up -> cc_replica1_caught_up -> cc_high_watermark -> time_broker -> b_produce_ok -> observe_committed_pending_cut -> time_client -> fe_recover -> c_poll>
```

## N03 — 先已提交且链路中断，随后超时，最后恢复并送达迟到响应。

结果：VALID；[原始日志](results/ClientNetwork.txt)。

```text
<init -> c_begin -> b_accept -> fe_client_cut -> b_dispatch -> b_io_begin -> b_io_complete -> cc_replica2_caught_up -> cc_replica1_caught_up -> cc_high_watermark -> time_broker -> b_produce_ok -> observe_committed_pending_cut -> time_client -> c_poll -> observe_timeout_during_cut -> fe_recover -> b_deliver>
```

## N04 — 错误的业务推断：超时就代表消息未提交。

结果：NOT valid；[原始日志](results/ClientNetwork.txt)。

```text
<init -> time_broker -> time_client -> fe_client_cut -> time_broker -> time_client -> fe_recover -> time_broker -> time_client -> c_poll -> c_begin -> time_broker -> time_client -> time_broker -> b_accept -> time_broker -> time_client -> c_poll -> time_broker -> time_client -> b_dispatch -> time_broker -> time_client -> c_poll -> b_io_begin -> time_broker -> time_client -> c_poll -> b_io_complete -> cc_replica2_caught_up -> cc_replica1_caught_up -> cc_high_watermark>
```

## N05 — 链路及时恢复后仍存在成功路径。

结果：VALID；[原始日志](results/ClientNetwork.txt)。

```text
<init -> fe_client_cut -> fe_recover -> c_begin -> b_accept -> b_dispatch -> b_io_begin -> b_io_complete -> cc_replica2_caught_up -> cc_replica1_caught_up -> cc_high_watermark -> time_broker -> b_produce_ok -> b_deliver -> c_poll>
```

## E01 — 请求已接收且仍等应用结果、执行停滞，新 LP 成功两次而 Ready 保持。

结果：VALID；[原始日志](results/Execution.txt)。

```text
<init -> fe_execution_stall -> c_begin -> b_accept -> lp_sample -> k_liveness_commit -> lp_sample>
```

## E02 — 候选健康契约：pgrep 成功应代表处理线程可推进；该蕴含不成立。

结果：NOT valid；[原始日志](results/Execution.txt)。

```text
<init -> time_broker -> time_client -> fe_execution_stall -> time_broker -> time_client -> c_poll -> c_begin -> time_broker -> time_client -> time_broker -> c_poll -> time_broker -> time_client -> lp_sample>
```

## D01 — 请求仍等应用结果且持锁等待存储，新 LP 成功两次而 Ready 保持。

结果：VALID；[原始日志](results/Storage.txt)。

```text
<init -> fe_storage_stall -> c_begin -> b_accept -> b_dispatch -> b_io_begin -> observe_pending_io -> lp_sample -> k_liveness_commit -> lp_sample>
```

## D02 — 候选健康契约：Ready 应蕴含存储可推进。

结果：NOT valid；[原始日志](results/Storage.txt)。

```text
<init -> time_broker -> time_client -> fe_storage_stall>
```

## D03 — 先观察调用仍在等待且 Broker 困于 I/O，随后 Producer 独立交付超时。

结果：VALID；[原始日志](results/Storage.txt)。

```text
<init -> fe_storage_stall -> c_begin -> b_accept -> b_dispatch -> b_io_begin -> observe_pending_io -> time_client -> c_poll>
```

## R01 — 存在追加和超期未追平条件时，ISR 降至 1，持续 RP 成功。

结果：VALID；[原始日志](results/Replica.txt)。

```text
<init -> fe_replica_cut -> cc_background_append -> cc_replica2_lag_elapsed -> cc_remove_replica2 -> cc_replica1_lag_elapsed -> cc_remove_replica1 -> c_begin -> time_client -> c_poll -> lp_sample -> k_liveness_commit -> lp_sample -> k_liveness_commit -> rp_sample -> k_readiness_commit -> lp_sample -> rp_sample>
```

## R02 — Ready 状态下客户端实际收到 ISR 不足协议错误。

结果：VALID；[原始日志](results/Replica.txt)。

```text
<init -> fe_replica_cut -> cc_background_append -> cc_replica2_lag_elapsed -> cc_remove_replica2 -> cc_replica1_lag_elapsed -> cc_remove_replica1 -> c_begin -> b_accept -> b_dispatch -> b_reject_isr -> b_deliver -> c_poll>
```

## R03 — HW 已覆盖记录后、检查 DelayedProduce 前 ISR 缩小，可返回 AFTER_APPEND。

结果：VALID；[原始日志](results/Replica.txt)。

```text
<init -> c_begin -> time_client -> b_accept -> c_poll -> b_dispatch -> b_io_begin -> b_io_complete -> cc_replica2_caught_up -> cc_replica1_caught_up -> cc_high_watermark -> time_broker -> fe_replica_cut -> cc_background_append -> cc_replica2_lag_elapsed -> cc_remove_replica2 -> cc_replica1_lag_elapsed -> cc_remove_replica1 -> b_after_append_error>
```

## R04 — 候选健康契约：Ready 蕴含本地分区满足最低 ISR。

结果：NOT valid；[原始日志](results/Replica.txt)。

```text
<init -> time_broker -> time_client -> fe_replica_cut -> cc_background_append -> cc_replica2_lag_elapsed -> cc_remove_replica2 -> cc_replica1_lag_elapsed -> cc_remove_replica1>
```

## R05 — 错误的业务推断：Broker 返回错误就代表记录未提交。

结果：NOT valid；[原始日志](results/Replica.txt)。

```text
<init -> time_broker -> time_client -> c_poll -> c_begin -> time_broker -> time_client -> time_broker -> b_accept -> time_broker -> time_client -> c_poll -> time_broker -> time_client -> b_dispatch -> time_broker -> time_client -> c_poll -> b_io_begin -> time_broker -> time_client -> c_poll -> b_io_complete -> cc_replica2_caught_up -> cc_replica1_caught_up -> cc_high_watermark -> time_broker -> time_client -> fe_replica_cut -> cc_background_append -> cc_replica2_lag_elapsed -> cc_remove_replica2 -> cc_replica1_lag_elapsed -> cc_remove_replica1 -> time_broker -> time_client -> c_poll -> b_after_append_error>
```

## Q01 — Readiness 连续失败未到六次时缓存 Ready 可以保留。

结果：VALID；[原始日志](results/ProbeNetwork.txt)。

```text
<init -> fe_probe_cut -> c_begin -> time_client -> c_poll -> lp_sample -> k_liveness_commit -> lp_sample -> k_liveness_commit -> rp_sample -> k_readiness_commit>
```

## Q02 — 先在 NotReady 状态观察仍等待的调用和成功响应，随后调用返回成功。

结果：VALID；[原始日志](results/ProbeNetwork.txt)。

```text
<init -> fe_probe_cut -> c_begin -> b_accept -> b_dispatch -> b_io_begin -> b_io_complete -> cc_replica2_caught_up -> cc_replica1_caught_up -> cc_high_watermark -> time_broker -> b_produce_ok -> b_deliver -> lp_sample -> k_liveness_commit -> lp_sample -> k_liveness_commit -> rp_sample -> k_readiness_commit -> lp_sample -> rp_sample -> k_readiness_commit -> k_liveness_commit -> rp_sample -> k_readiness_commit -> lp_sample -> rp_sample -> k_readiness_commit -> k_liveness_commit -> rp_sample -> k_readiness_commit -> lp_sample -> rp_sample -> k_readiness_commit -> observe_notready_pending -> c_poll>
```

## Q03 — 反向健康契约：NotReady 以后不应还能完成业务成功；独立路径可反驳。

结果：NOT valid；[原始日志](results/ProbeNetwork.txt)。

```text
<init -> time_broker -> time_client -> fe_probe_cut -> time_broker -> time_client -> c_poll -> c_begin -> time_broker -> c_poll -> b_accept -> time_broker -> c_poll -> b_dispatch -> time_broker -> c_poll -> b_io_begin -> time_broker -> c_poll -> b_io_complete -> cc_replica2_caught_up -> cc_replica1_caught_up -> cc_high_watermark -> time_broker -> c_poll -> b_produce_ok -> time_broker -> c_poll -> b_deliver -> time_broker -> lp_sample -> time_broker -> k_liveness_commit -> time_broker -> lp_sample -> time_broker -> k_liveness_commit -> time_broker -> rp_sample -> time_broker -> k_readiness_commit -> time_broker -> lp_sample -> time_broker -> k_liveness_commit -> rp_sample -> time_broker -> k_readiness_commit -> time_broker -> lp_sample -> time_broker -> k_liveness_commit -> rp_sample -> time_broker -> k_readiness_commit -> time_broker -> lp_sample -> time_broker -> k_liveness_commit -> rp_sample -> time_broker -> k_readiness_commit -> time_broker -> lp_sample -> time_broker -> k_liveness_commit -> rp_sample -> time_broker -> k_readiness_commit -> time_broker -> lp_sample -> time_broker -> k_liveness_commit -> rp_sample -> time_broker -> k_readiness_commit -> observe_notready_pending -> time_broker -> c_poll>
```

## F01 — 健康已提交数据存在时可以完成有数据的 poll。

结果：VALID；[原始日志](results/FetchData.txt)。

```text
<init -> c_begin -> b_accept -> b_dispatch -> b_io_begin -> b_io_complete -> time_broker -> b_fetch_data -> b_deliver -> c_poll>
```

## F03 — 无目标数据时 poll 正常返回空集合。

结果：VALID；[原始日志](results/FetchEmpty.txt)。

```text
<init -> c_begin -> time_client -> c_poll>
```

## F05 — Broker 存有数据但 poll 截止时仍可无数据返回。

结果：VALID；[原始日志](results/FetchNetwork.txt)。

```text
<init -> fe_client_cut -> c_begin -> time_client -> c_poll>
```

## F06 — 错误的业务推断：poll 返回空集合代表 Broker 没有数据。

结果：NOT valid；[原始日志](results/FetchNetwork.txt)。

```text
<init -> time_broker -> time_client -> fe_client_cut -> time_broker -> time_client -> c_poll -> c_begin -> time_broker -> time_client -> time_broker -> c_poll>
```

## M01 — Leader 迁离后旧路由 Fetch 可收到协议错误并内部重试。

结果：VALID；[原始日志](results/Metadata.txt)。

```text
<init -> fe_metadata_move -> cc_leader_move -> c_begin -> b_accept -> b_dispatch -> b_not_leader -> b_deliver -> c_poll>
```
