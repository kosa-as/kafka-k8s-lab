$ErrorActionPreference = 'Stop'
$DeployRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$Tokens = $null
$ParseErrors = $null
$Ast = [System.Management.Automation.Language.Parser]::ParseFile((Join-Path $DeployRoot 'logging.ps1'), [ref]$Tokens, [ref]$ParseErrors)
if ($ParseErrors.Count) { throw 'logging.ps1 did not parse' }
foreach ($Name in @('Test-KafkaPodShape', 'Wait-WebhookAdmission')) {
    $Function = $Ast.Find({ param($Node) $Node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $Node.Name -eq $Name }, $true)
    Invoke-Expression $Function.Extent.Text
}

# Simulate API-server propagation without changing cluster TLS or creating Pods.
$Admitted = @{
    spec = @{
        containers = @(
            @{ name = 'kafka'; image = 'kafka-health:local'; volumeMounts = @(@{ name = 'kafka-health'; mountPath = '/var/run/kafka-health'; readOnly = $true }) },
            @{ name = 'log-agent' },
            @{ name = 'health-sidecar'; volumeMounts = @(@{ name = 'kafka-health'; mountPath = '/var/run/kafka-health'; readOnly = $false }) }
        )
        volumes = @(@{ name = 'kafka-health'; emptyDir = @{} })
    }
} | ConvertTo-Json -Depth 10
function Start-Sleep { param($Seconds) }
function kubectl {
    $script:Calls++
    if ($script:AlwaysFail -or $script:Calls -eq 1) {
        $global:LASTEXITCODE = 1
        Write-Error 'simulated TLS propagation failure'
        return
    }
    $global:LASTEXITCODE = 0
    $Admitted
}
$script:Calls = 0
$script:AlwaysFail = $false
Wait-WebhookAdmission
if ($script:Calls -ne 2) { throw 'Admission did not recover on the second attempt' }
Write-Host 'PASS: admission recovers after a transient failure.'

$script:Calls = 0
$script:AlwaysFail = $true
$Failure = $null
try { Wait-WebhookAdmission } catch { $Failure = $_.Exception.Message }
if ($script:Calls -ne 30 -or $Failure -notlike '*simulated TLS propagation failure*') {
    throw 'Admission must exhaust retries and preserve the last diagnostic'
}
Write-Host 'PASS: persistent admission failure retains diagnostics after bounded retries.'
