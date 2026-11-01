[CmdletBinding()]
param(
    [string]$Drive = "Z:",
    [string]$RemotePath = "\\fileserver\market_data",
    [string]$DockerMountPath = "/mnt/host/pipeline-smb",
    [string]$DataVolume = "pipeline_smb_data",
    [int]$DockerStartupTimeoutSeconds = 120,
    [switch]$RegisterWatchdog,
    [switch]$SkipCompose
)

$ErrorActionPreference = "Stop"
$mutex = [Threading.Mutex]::new($false, "Local\PipelineSmbDockerMount")
$hasMutex = $false

function Resolve-DockerCli {
    $command = Get-Command docker -ErrorAction SilentlyContinue
    if ($command) {
        return $command.Source
    }

    $candidate = Join-Path $env:LOCALAPPDATA "Programs\DockerDesktop\resources\bin\docker.exe"
    if (Test-Path -LiteralPath $candidate) {
        return $candidate
    }

    throw "Docker CLI를 찾을 수 없습니다. Docker Desktop 설치 상태를 확인하세요."
}

function Wait-DockerEngine {
    param(
        [Parameter(Mandatory)] [string]$DockerCli,
        [Parameter(Mandatory)] [int]$TimeoutSeconds
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        if (Test-DockerEngine -DockerCli $DockerCli) {
            return
        }
        Start-Sleep -Seconds 2
    }

    throw "Docker Desktop 엔진이 ${TimeoutSeconds}초 안에 준비되지 않았습니다."
}

function Test-DockerEngine {
    param([Parameter(Mandatory)] [string]$DockerCli)

    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "SilentlyContinue"
        & $DockerCli info *> $null
        return $LASTEXITCODE -eq 0
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
}

function Test-DockerVolume {
    param(
        [Parameter(Mandatory)] [string]$DockerCli,
        [Parameter(Mandatory)] [string]$Name
    )

    $previousErrorActionPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "SilentlyContinue"
        & $DockerCli volume inspect $Name *> $null
        return $LASTEXITCODE -eq 0
    }
    finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
}

try {
    $hasMutex = $mutex.WaitOne(0)
    if (-not $hasMutex) {
        Write-Host "다른 SMB 시작 작업이 이미 실행 중입니다."
        exit 0
    }

    $driveRoot = "$Drive\"
    if (-not (Test-Path -LiteralPath $driveRoot)) {
        & net.exe use $Drive $RemotePath /persistent:yes | Out-Null
        if ($LASTEXITCODE -ne 0 -or -not (Test-Path -LiteralPath $driveRoot)) {
            throw "$Drive -> $RemotePath SMB 연결에 실패했습니다. Windows 자격 증명과 네트워크를 확인하세요."
        }
    }

    if (-not (Test-Path -LiteralPath (Join-Path $driveRoot "MFT_raw"))) {
        throw "$driveRoot 경로에 MFT_raw 폴더가 없습니다. $RemotePath 공유가 맞는지 확인하세요."
    }

    $docker = Resolve-DockerCli
    if (-not (Test-DockerEngine -DockerCli $docker)) {
        & $docker desktop start | Out-Null
    }
    Wait-DockerEngine -DockerCli $docker -TimeoutSeconds $DockerStartupTimeoutSeconds

    wsl.exe -d docker-desktop -u root --exec mountpoint -q $DockerMountPath
    $mountWasCreated = $LASTEXITCODE -ne 0
    if ($mountWasCreated) {
        wsl.exe -d docker-desktop -u root --exec install -d -m 0755 $DockerMountPath
        if ($LASTEXITCODE -ne 0) {
            throw "Docker Desktop VM의 SMB 마운트 지점 생성에 실패했습니다."
        }

        wsl.exe -d docker-desktop -u root --exec mount -t drvfs $Drive $DockerMountPath
        if ($LASTEXITCODE -ne 0) {
            throw "$Drive 드라이브를 Docker Desktop VM에 drvfs로 마운트하지 못했습니다."
        }
    }

    wsl.exe -d docker-desktop -u root --exec test -d "$DockerMountPath/MFT_raw"
    if ($LASTEXITCODE -ne 0) {
        throw "Docker Desktop VM에서 실제 SMB 데이터 디렉터리를 확인하지 못했습니다."
    }

    if (-not (Test-DockerVolume -DockerCli $docker -Name $DataVolume)) {
        & $docker volume create `
            --driver local `
            --opt type=none `
            --opt "device=$DockerMountPath" `
            --opt o=bind `
            $DataVolume | Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw "Docker SMB named volume '$DataVolume' 생성에 실패했습니다."
        }
    }

    & $docker run --rm `
        --mount "type=volume,source=$DataVolume,target=/data_db,readonly" `
        --entrypoint /bin/sh postgres:16 `
        -c "test -d /data_db/MFT_raw"
    if ($LASTEXITCODE -ne 0) {
        throw "Docker SMB named volume 읽기 검증에 실패했습니다."
    }

    if ($RegisterWatchdog) {
        $taskName = "Data Pipeline SMB Docker Watchdog"
        $taskCommand = "powershell.exe -NoProfile -ExecutionPolicy Bypass -File `"$PSCommandPath`""
        & schtasks.exe /Create /TN $taskName /TR $taskCommand /SC MINUTE /MO 5 /F | Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw "Windows 예약 작업 '$taskName' 등록에 실패했습니다."
        }

        $taskSettings = New-ScheduledTaskSettingsSet `
            -AllowStartIfOnBatteries `
            -DontStopIfGoingOnBatteries `
            -StartWhenAvailable `
            -ExecutionTimeLimit (New-TimeSpan -Minutes 10) `
            -MultipleInstances IgnoreNew
        Set-ScheduledTask -TaskName $taskName -Settings $taskSettings | Out-Null
        Write-Host "Windows 예약 작업 등록 완료: $taskName (5분 간격)"
    }

    if (-not $SkipCompose) {
        $composeDirectory = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot "..\docker"))
        Push-Location $composeDirectory
        try {
            $composeArguments = @("compose", "up", "-d")
            if ($mountWasCreated) {
                $composeArguments += "--force-recreate"
            }
            & $docker @composeArguments
            if ($LASTEXITCODE -ne 0) {
                throw "Docker Compose stack 시작에 실패했습니다."
            }
        }
        finally {
            Pop-Location
        }
    }

    Write-Host "SMB 데이터 볼륨 준비 완료: $RemotePath -> $DataVolume -> /data_db"
}
finally {
    if ($hasMutex) {
        $mutex.ReleaseMutex()
    }
    $mutex.Dispose()
}
