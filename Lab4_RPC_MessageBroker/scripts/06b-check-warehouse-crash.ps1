param(
    [string]$Dataset = 'data/orders.jsonl',
    [ValidateRange(10000, 1000000)][int]$Count = 10000,
    [ValidateRange(30, 7200)][int]$WaitTimeoutSeconds = 600
)
. "$PSScriptRoot\experiment-common.ps1"
Initialize-LabFailureWarehouse
Invoke-LabExperiment -Step '06b_warehouse_crash' -Module 'experiments.failures_broker' -NeedsDotnet -PythonArguments @(
    '--scenario', 'crash', '--dataset', $Dataset, '--count', "$Count", '--timeout', "$WaitTimeoutSeconds"
)
