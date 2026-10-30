. "$PSScriptRoot\experiment-common.ps1"
Invoke-LabExperiment -Step '04c_unknown_fields' -Module 'experiments.compatibility' -NeedsDotnet -PythonArguments @(
    '--part', 'relay'
)
