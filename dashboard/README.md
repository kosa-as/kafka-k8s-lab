# Kubernetes Dashboard

本文记录当前 Docker Desktop Kubernetes 集群中的 Kubernetes Dashboard 安装状态、访问方式和维护命令。

## 当前部署

- Kubernetes context：`docker-desktop`
- Namespace：`kubernetes-dashboard`
- Helm Release：`kubernetes-dashboard`
- Chart：`kubernetes-dashboard` `7.14.0`
- 网关：Kong，Service 类型为 `NodePort`
- HTTPS NodePort：`30443`
- 浏览器地址：<https://localhost:30443/>
- Dashboard 各组件运行在 `docker-desktop` 节点；当前已全部 Ready。

安装前集群中没有 Dashboard 工作负载或相关 Dashboard 镜像。安装 Chart 时 Kubernetes 从镜像仓库拉取并启动了 Dashboard Web、API、Auth、Metrics Scraper 和 Kong 镜像。

Dashboard 页面通过 HTTPS 提供服务，当前证书为自签名证书。首次打开时浏览器可能显示证书警告；仅在确认地址为本机集群后继续访问。

## 安装或更新

推荐使用统一部署入口：

```powershell
.\deploy\dashboard.ps1
```

完整平台部署请执行：

```powershell
.\deploy\all.ps1
```

下面是 `deploy/dashboard.ps1` 使用的底层安装命令，适合需要手动控制 Chart 下载或参数时参考。

Kubernetes Dashboard 官方 Helm 仓库地址当前返回 404。下面使用官方 GitHub Release 中的 Chart 包，固定到本次部署使用的 `7.14.0`：

```powershell
$chartRoot = Join-Path $env:TEMP 'kubernetes-dashboard-chart'
New-Item -ItemType Directory -Force $chartRoot | Out-Null
helm pull https://github.com/kubernetes/dashboard/releases/download/kubernetes-dashboard-7.14.0/kubernetes-dashboard-7.14.0.tgz `
  --untar --untardir $chartRoot

helm upgrade --install kubernetes-dashboard `
  (Join-Path $chartRoot 'kubernetes-dashboard') `
  --namespace kubernetes-dashboard `
  --create-namespace `
  --set kong.proxy.type=NodePort `
  --set kong.proxy.tls.nodePort=30443 `
  --set kong.proxy.http.enabled=false `
  --wait --timeout 10m
```

等待所有部署可用：

```powershell
kubectl wait --for=condition=Available deployment --all `
  -n kubernetes-dashboard --timeout=8m
kubectl get pods,svc -n kubernetes-dashboard -o wide
```

## 访问与登录

打开 <https://localhost:30443/>，选择 Token 登录。当前用于本地管理的 ServiceAccount 是 `dashboard-admin`，并绑定了 `cluster-admin`。

已为该 ServiceAccount 创建一个手工维护的长期 Token Secret：

- 清单：`dashboard/10-dashboard-admin-token.yaml`
- Secret：`kubernetes-dashboard/dashboard-admin-token`
- 类型：`kubernetes.io/service-account-token`
- 该 Token 不包含 `exp` 过期字段，不会像 `kubectl create token` 生成的短期 Token 一样自动过期。

获取长期 Token：

```powershell
$token = kubectl -n kubernetes-dashboard get secret dashboard-admin-token `
  -o jsonpath="{.data.token}"
[Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($token))
```

将命令输出的令牌粘贴到 Dashboard 登录页。不要把 Token 提交到代码库、聊天记录或长期文档中。

应用或重新应用 Token Secret：

```powershell
kubectl apply -f .\dashboard\10-dashboard-admin-token.yaml
```

该长期 Token 只有在删除 Secret/ServiceAccount、撤销 RBAC 权限、轮换集群 ServiceAccount 签名密钥或重建集群时才会失效。需要立即撤销时执行：

```powershell
kubectl delete secret dashboard-admin-token -n kubernetes-dashboard
```

`cluster-admin` 可以管理整个集群，权限极高。此绑定是为了当前本地管理方便而创建；不要将该 Dashboard 端口暴露到不可信网络。多人或长期使用时，应为各用户创建最小权限 RBAC，而不是共享这个管理员身份。

如需重新创建当前本地管理员身份：

```powershell
kubectl create serviceaccount dashboard-admin -n kubernetes-dashboard
kubectl create clusterrolebinding dashboard-admin `
  --clusterrole=cluster-admin `
  --serviceaccount=kubernetes-dashboard:dashboard-admin
kubectl apply -f .\dashboard\10-dashboard-admin-token.yaml
```

## 固定端口与临时端口转发

当前通过 Service NodePort 暴露 HTTPS，不需要一直运行 `kubectl port-forward`：

```powershell
kubectl get svc kubernetes-dashboard-kong-proxy `
  -n kubernetes-dashboard `
  -o wide
```

预期端口为 `443:30443/TCP`。本地访问地址是 `https://localhost:30443/`。如需使用临时端口转发，可改用：

```powershell
kubectl port-forward -n kubernetes-dashboard `
  svc/kubernetes-dashboard-kong-proxy 8443:443
```

此时访问 <https://localhost:8443/>；端口转发进程停止后该地址即不可用。

## 验证

```powershell
helm list -n kubernetes-dashboard
kubectl get deployments -n kubernetes-dashboard
kubectl get pods,svc -n kubernetes-dashboard -o wide
curl.exe -k -I --connect-timeout 5 --max-time 15 https://localhost:30443/
```

成功时所有 Deployment 的 `AVAILABLE` 为 `1`，Service 显示 `443:30443/TCP`，HTTP 检查返回 `200`。`curl -k` 仅跳过本次自签名证书校验，不代表适合生产环境使用。

## 卸载

下面命令会移除 Dashboard Helm Release 和本地管理员授权；执行前确认不再需要该控制台：

```powershell
helm uninstall kubernetes-dashboard -n kubernetes-dashboard
kubectl delete clusterrolebinding dashboard-admin
kubectl delete serviceaccount dashboard-admin -n kubernetes-dashboard
kubectl delete secret dashboard-admin-token -n kubernetes-dashboard
kubectl delete namespace kubernetes-dashboard
```

## 生命周期说明

Kubernetes Dashboard 上游项目已归档并停止维护。当前集群运行的是官方发布的 `7.14.0` Chart；后续使用前应评估安全更新和兼容性。上游 README 建议新部署评估 Kubernetes SIG UI 的 [Headlamp](https://github.com/kubernetes-sigs/headlamp)。

参考：

- [Kubernetes Dashboard 上游仓库及维护状态](https://github.com/kubernetes/dashboard)
- [Dashboard 官方访问控制说明](https://github.com/kubernetes/dashboard/blob/master/docs/user/access-control/README.md)
- [Dashboard 7.14.0 Release Chart](https://github.com/kubernetes/dashboard/releases/download/kubernetes-dashboard-7.14.0/kubernetes-dashboard-7.14.0.tgz)
