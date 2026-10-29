param(
    [ValidateRange(5, 100)][int]$Runs = 5,
    [string]$Dataset = 'data/orders.jsonl'
)
. "$PSScriptRoot\experiment-common.ps1"
Invoke-LabExperiment -Step '02_codec' -Module 'experiments.codec' -PythonArguments @(
    '--runs', "$Runs", '--dataset', $Dataset
)
