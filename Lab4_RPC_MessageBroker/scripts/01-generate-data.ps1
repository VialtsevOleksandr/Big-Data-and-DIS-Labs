param(
    [ValidateRange(10000, 1000000)][int]$Count = 10000,
    [int]$Seed = 20050406,
    [string]$Dataset = 'data/orders.jsonl'
)
. "$PSScriptRoot\experiment-common.ps1"
Invoke-LabExperiment -Step '01_dataset' -Module 'experiments.generate' -PythonArguments @(
    '--count', "$Count", '--seed', "$Seed", '--dataset', $Dataset
)
