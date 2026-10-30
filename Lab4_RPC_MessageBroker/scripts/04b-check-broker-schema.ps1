param([string]$BrokerUrl = 'amqp://dis:dis-local@127.0.0.1:5673/%2F')
. "$PSScriptRoot\experiment-common.ps1"
Invoke-LabExperiment -Step '04b_broker_schema' -Module 'experiments.compatibility' -NeedsDotnet -PythonArguments @(
    '--part', 'broker', '--broker-url', $BrokerUrl
)
