# 一二章审查意见落实记录

作者授权：“按照你的审查报告去做一下更改，我觉得你提的这些没有问题”。

保留原审查报告作为历史记录，本文件记录落实情况。本次采用原 DOCX 段落锚点进行局部替换，保留修改前备份及逐段差异；不声称通过 ARS 全流程完整性认证。正文未含摘要，本次未生成摘要。

| 意见 | 处理 |
|---|---|
| C01 | 1.1加入所选部署中进程/端口与写入条件分离的示例，明确为待验证情形 |
| C02 | 1.2明确目标、分区、角色、配置、窗口和三类能力，客户端结果单列 |
| C03 | 独立说明固定平台处置条件与业务表现观测的关系 |
| C04 | 1.3将增量落实为观测对应、判定分流和证据对照，不宣称计数理论创新 |
| C05 | 1.4增加延迟时间点、独立真值、UNKNOWN及P99口径，细节留第五章 |
| C06 | 1.5第二章顺序按实际结构修正 |
| C07 | 2.1.1增加Service/元数据直连/在途请求的适用限制 |
| C08 | 改用“gRPC健康检查”，删除错误英文展开 |
| C09 | 2.1.3区分Chart、values、Topic、客户端配置及Pod快照来源 |
| C10 | 2.3.2增加横向综合段，比较原生探针、业务请求、OmegaGen/Panorama/Dapper及本文对应任务 |
| C11 | 明确部署2.8.4与功能文档2.6.7的差异，增加故障注入与模型映射限制 |
| C12 | 2.3.4补充本地身份、阶段、ISR新鲜度与独立采集的验证条件 |
| C13 | 合并基础符号说明，突出实际交错模型，消除公平性介绍重复 |
| C14 | 统一35条文献样式和左对齐，补核[31]卷期页码与DOI、[28]页码；核对关键数值的官方来源 |
| 跨章提示 | 修正3.5对第四/五章的指代；表4.5明确重启不保证修复外部故障 |

来源核对：[云计算蓝皮书原PDF](https://www.caict.ac.cn/kxyj/qwfb/bps/202507/P020250722583603558109.pdf)、[CNCF原报告](https://www.cncf.io/wp-content/uploads/2026/01/CNCF_Annual_Survey_Report_final.pdf)、[OmegaGen官方记录](https://www.usenix.org/conference/nsdi20/presentation/lou)、[Panorama官方会议摘要](https://www.usenix.org/conference/osdi18/technical-sessions)、[AWS作者机构记录](https://www.amazon.science/publications/how-amazon-web-services-uses-formal-methods)、[AWS原论文PDF](https://assets.amazon.science/67/f9/92733d574c11ba1a11bd08bfb8ae/how-amazon-web-services-uses-formal-methods.pdf)、[Metastable官方记录](https://www.usenix.org/conference/osdi22/presentation/huang-lexiang)。

引文核验范围：重点数字、比较段涉及的既有论述及上述补充字段；并非35条全文逐句审计。USENIX部分PDF直接访问失败，实验数字由同篇官方摘要交叉核对；未把其他论文或版本冒充被引用全文。其余尚缺出版字段不补造，后续定稿仍需完整著录核查。

缩写脚本为部分覆盖：不能可靠解析HTTP/ISR/JMX部分括号，且把模型代码标识和工具名称报为缩写。报告保存在本次工作记录中；未据此改动未经本次审查涉及的三四章术语。

检查：18张表、13幅图保留；正文35个引用编号均有文献条目；未指定修改的非参考文献段落XML保持一致。使用Microsoft Word只读导出检查分页，原打包LibreOffice渲染器不可用。未运行新模型或实验，已有结果数字不变。
