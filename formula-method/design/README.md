# CSP 建模与验证入口

当前整体版本为 **[M0-R2](M0/README.md)**（2026-10-21）。它重新组织七个角色的状态、组合和验证入口，修正原有副本确认、ISR 缩减和客户端期限抽象。

- [模型结构与约束](M0/DESIGN.md)
- [源码到模型的对应及旧模型问题](M0/SOURCE_AUDIT.md)
- [逐项性质和实际结果](M0/VERIFICATION.md)
- [反例、缺陷候选与证据边界](M0/FINDINGS.md)
- [可复跑的检查入口](M0/checks/Healthy.csp)

原来的七个角色目录保留为 **2026-10-20 历史基线**，原始日志与实机只读快照仍可查阅。原 [VERIFICATION_PROPERTIES.md](VERIFICATION_PROPERTIES.md) 的 133 条声明、288 次执行属于历史版本，不能与新版结果相加。新版不通过包含旧测试入口继承断言。
