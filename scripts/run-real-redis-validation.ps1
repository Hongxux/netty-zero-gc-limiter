param(
    [int]$RedisPort = 6379,
    [int]$Threads = 16,
    [int]$OpsPerThread = 2000
)

$ErrorActionPreference = 'Stop'
$containerName = "netty-limiter-redis-$PID"
$root = Split-Path -Parent $PSScriptRoot

try {
    docker version | Out-Null
    docker run --name $containerName -d -p "${RedisPort}:6379" redis:7.2-alpine | Out-Null
    $ready = $false
    for ($i = 0; $i -lt 30; $i++) {
        try {
            if ((docker exec $containerName redis-cli ping).Trim() -eq 'PONG') {
                $ready = $true
                break
            }
        } catch {
            # Container is still starting.
        }
        Start-Sleep -Seconds 1
    }
    if (-not $ready) {
        throw "Redis container did not become ready within 30 seconds"
    }

    $env:REDIS_HOST = '127.0.0.1'
    $env:REDIS_PORT = "$RedisPort"
    Push-Location $root
    try {
        mvn ("-Dbench.threads=$Threads") ("-Dbench.ops=$OpsPerThread") '-Dtest=RealRedisRateLimiterIntegrationTest,RedisLimiterComparisonBenchmarkTest' test
        if ($LASTEXITCODE -ne 0) {
            throw "Maven validation failed with exit code $LASTEXITCODE"
        }
    } finally {
        Pop-Location
    }
    Write-Host "Results: $root\target\perf-results\redis-limiter-comparison.csv"
} finally {
    docker rm -f $containerName 2>$null | Out-Null
}
