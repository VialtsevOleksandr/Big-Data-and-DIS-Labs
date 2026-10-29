param(
    [string]$Address = '127.0.0.1:50051',
    [ValidateRange(0, 3600000)][int]$CreateResponseDelayMs = 0,
    [switch]$DisableIdempotency,
    [ValidateSet('v1','v2','v2_breaking')][string]$Schema = 'v1',
    [switch]$WithBroker,
    [string]$Namespace = 'lab'
)
. "$PSScriptRoot\common.ps1"
Assert-LabPython
Push-Location $LabRoot
try {
    $serverArguments = @('-m', 'server.main', '--address', $Address, '--create-response-delay-ms', $CreateResponseDelayMs, '--schema', $Schema)
    if ($WithBroker) { $serverArguments += @('--broker-url', 'amqp://dis:dis-local@127.0.0.1:5673/%2F', '--namespace', $Namespace) }
    if ($DisableIdempotency) { $serverArguments += '--disable-idempotency' }
    & $LabPython @serverArguments
    if ($LASTEXITCODE -ne 0) { throw 'Order service exited with an error.' }
} finally {
    Pop-Location
}
