param(
    [ValidateRange(100, 60000)][int]$DelayMs = 2000,
    [ValidateRange(0.01, 3600.0)][double]$TimeoutSeconds = 0.5,
    [ValidateRange(0.01, 3600.0)][double]$RetryTimeoutSeconds = 10
)
. "$PSScriptRoot\experiment-common.ps1"
Invoke-LabExperiment -Step '05_rpc_failures' -Module 'experiments.failures_rpc' -NeedsDotnet -PythonArguments @(
    '--delay-ms', "$DelayMs", '--timeout', $TimeoutSeconds.ToString([Globalization.CultureInfo]::InvariantCulture),
    '--retry-timeout', $RetryTimeoutSeconds.ToString([Globalization.CultureInfo]::InvariantCulture)
)
