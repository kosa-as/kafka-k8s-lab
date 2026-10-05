$ErrorActionPreference = 'Stop'

$ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $ScriptRoot
$DashboardRoot = Join-Path $RepoRoot 'dashboard'
$ChartVersion = '7.14.0'
$ChartUrl = "https://github.com/kubernetes/dashboard/releases/download/kubernetes-dashboard-$ChartVersion/kubernetes-dashboard-$ChartVersion.tgz"
$ChartCacheRoot = Join-Path $DashboardRoot '.helm-cache'
$ChartDir = Join-Path $ChartCacheRoot $ChartVersion
$ChartPath = Join-Path $ChartDir 'kubernetes-dashboard'

New-Item -ItemType Directory -Path $ChartDir -Force | Out-Null
# Inspect optional native-command failures by exit code. In Windows PS 5.1,
# direct stderr redirection can terminate before the fallback is evaluated.
$HelmExe = (Get-Command helm -ErrorAction Stop).Source
$HelmStatus = Start-Process -FilePath $HelmExe `
  -ArgumentList @('status', 'kubernetes-dashboard', '-n', 'kubernetes-dashboard') `
  -NoNewWindow -Wait -PassThru `
  -RedirectStandardOutput (Join-Path $ChartDir 'helm-status.stdout.log') `
  -RedirectStandardError (Join-Path $ChartDir 'helm-status.stderr.log')
$ReleaseExists = ($HelmStatus.ExitCode -eq 0)

$ChartManifestPath = Join-Path $ChartPath 'Chart.yaml'
$ChartAvailable = Test-Path -LiteralPath $ChartManifestPath
$HelmPullError = ''
if (-not $ChartAvailable) {
    Write-Host "Downloading Kubernetes Dashboard chart $ChartVersion..."
    Remove-Item -LiteralPath $ChartPath -Recurse -Force -ErrorAction SilentlyContinue
    $HelmPullErrorPath = Join-Path $ChartDir 'helm-pull.stderr.log'
    $HelmPull = Start-Process -FilePath $HelmExe `
      -ArgumentList @('pull', ('"{0}"' -f $ChartUrl), '--untar', '--untardir', ('"{0}"' -f $ChartDir)) `
      -NoNewWindow -Wait -PassThru `
      -RedirectStandardError $HelmPullErrorPath
    $ChartAvailable = ($HelmPull.ExitCode -eq 0 -and (Test-Path -LiteralPath $ChartManifestPath))
    $HelmPullError = Get-Content -Raw -LiteralPath $HelmPullErrorPath
}
if (-not $ChartAvailable -and -not $ReleaseExists) {
    throw "Failed to download or unpack Kubernetes Dashboard chart ${ChartVersion}: $HelmPullError"
}

if ($ChartAvailable) {
    helm upgrade --install kubernetes-dashboard $ChartPath `
      --namespace kubernetes-dashboard `
      --create-namespace `
      --set kong.proxy.type=NodePort `
      --set kong.proxy.tls.nodePort=30443 `
      --set kong.proxy.http.enabled=false `
      --wait `
      --timeout 10m
    if ($LASTEXITCODE -ne 0) {
        throw 'Kubernetes Dashboard Helm upgrade failed'
    }
}
else {
    Write-Warning "Dashboard Helm chart download failed; reusing the existing Helm release. $HelmPullError"
}

kubectl create serviceaccount dashboard-admin `
  -n kubernetes-dashboard `
  --dry-run=client -o yaml | kubectl apply -f -
kubectl create clusterrolebinding dashboard-admin `
  --clusterrole=cluster-admin `
  --serviceaccount=kubernetes-dashboard:dashboard-admin `
  --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f (Join-Path $DashboardRoot '10-dashboard-admin-token.yaml')

kubectl wait --for=condition=Available deployment --all `
  -n kubernetes-dashboard `
  --timeout=8m
kubectl get pods,svc -n kubernetes-dashboard -o wide
