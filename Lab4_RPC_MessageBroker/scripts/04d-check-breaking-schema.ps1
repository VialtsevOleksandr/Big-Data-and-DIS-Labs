param([string]$BrokerUrl = 'amqp://dis:dis-local@127.0.0.1:5673/%2F')
. "$PSScriptRoot\experiment-common.ps1"
Invoke-LabExperiment -Step '04d_breaking_schema' -Module 'experiments.compatibility' -NeedsDotnet -PythonArguments @(
    '--part', 'breaking', '--broker-url', $BrokerUrl
)
