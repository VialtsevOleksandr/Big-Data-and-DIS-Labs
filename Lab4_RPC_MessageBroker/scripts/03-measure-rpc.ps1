param(
    [ValidateRange(5, 100)][int]$Runs = 5,
    [ValidateRange(0.01, 3600.0)][double]$TimeoutSeconds = 10,
    [string]$Dataset = 'data/orders.jsonl'
)
. "$PSScriptRoot\experiment-common.ps1"
Invoke-LabExperiment -Step '03_rpc' -Module 'experiments.rpc' -NeedsDotnet -PythonArguments @(
    '--runs', "$Runs", '--timeout', $TimeoutSeconds.ToString([Globalization.CultureInfo]::InvariantCulture),
    '--dataset', $Dataset
)
