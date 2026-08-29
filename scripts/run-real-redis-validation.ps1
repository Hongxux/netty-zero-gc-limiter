param(
    [int]$RedisPort = 6379,
    [int]$Threads = 16,
    [int]$OpsPerThread = 2000,
    [switch]$UseExistingRedis
)

$ErrorActionPreference = 'Stop'
$containerName = "netty-limiter-redis-$PID"
$root = Split-Path -Parent $PSScriptRoot
$containerCreated = $false

try {
    docker version | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "Docker engine is unavailable; start Docker Desktop Linux engine first"
    }
    if (-not $UseExistingRedis) {
        docker run --name $containerName -d -p "${RedisPort}:6379" redis:7.2-alpine | Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw "Unable to start Redis container on port $RedisPort"
        }
        $containerCreated = $true
    } else {
        Write-Host "Using existing Redis at 127.0.0.1:$RedisPort"
    }
    $ready = $false
    for ($i = 0; $i -lt 30; $i++) {
        try {
            $ping = if ($UseExistingRedis) {
                $tcp = [Net.Sockets.TcpClient]::new()
                try {
                    $tcp.Connect('127.0.0.1', $RedisPort)
                    'PONG'
                } finally {
                    $tcp.Dispose()
                }
            } else {
                docker exec $containerName redis-cli ping
            }
            if ($ping.Trim() -eq 'PONG') {
                $ready = $true
                break
            }
        } catch {
            # Container is still starting.
        }
        Start-Sleep -Seconds 1
    }
    if (-not $ready) {
        throw "Redis did not become ready at 127.0.0.1:$RedisPort within 30 seconds"
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
    if ($containerCreated) {
        docker rm -f $containerName 2>$null | Out-Null
    }
}
