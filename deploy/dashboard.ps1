$ErrorActionPreference = 'Stop'

$ScriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $ScriptRoot
$DashboardRoot = Join-Path $RepoRoot 'dashboard'
$ChartVersion = '7.14.0'
$ChartUrl = "https://github.com/kubernetes/dashboard/releases/download/kubernetes-dashboard-$ChartVersion/kubernetes-dashboard-$ChartVersion.tgz"
$ChartDir = Join-Path ([System.IO.Path]::GetTempPath()) `
  ('kubernetes-dashboard-chart-' + [guid]::NewGuid().ToString('N'))
$ChartPath = Join-Path $ChartDir 'kubernetes-dashboard'

New-Item -ItemType Directory -Path $ChartDir | Out-Null
try {
    helm status kubernetes-dashboard -n kubernetes-dashboard 2>$null | Out-Null
    $ReleaseExists = ($LASTEXITCODE -eq 0)

    Write-Host "Downloading Kubernetes Dashboard chart $ChartVersion..."
    helm pull $ChartUrl --untar --untardir $ChartDir 2>$null
    $ChartAvailable = ($LASTEXITCODE -eq 0 -and (Test-Path -LiteralPath $ChartPath))
    if (-not $ChartAvailable -and -not $ReleaseExists) {
        throw "Failed to download or unpack Kubernetes Dashboard chart $ChartVersion"
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
        Write-Warning 'Dashboard Helm chart download failed; reusing the existing Helm release.'
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
}
finally {
    Remove-Item -LiteralPath $ChartDir -Recurse -Force -ErrorAction SilentlyContinue
}
