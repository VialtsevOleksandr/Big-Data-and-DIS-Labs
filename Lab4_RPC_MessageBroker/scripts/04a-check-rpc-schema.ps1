. "$PSScriptRoot\experiment-common.ps1"
Invoke-LabExperiment -Step '04a_rpc_schema' -Module 'experiments.compatibility' -NeedsDotnet -PythonArguments @(
    '--part', 'rpc'
)
