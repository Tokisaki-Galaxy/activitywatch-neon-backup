param(
    [string]$TaskName = "ActivityWatch Neon Backup",
    [int]$IntervalHours = 1
)

$ErrorActionPreference = "Stop"
$dir = Split-Path -Parent $MyInvocation.MyCommand.Path
$vbs = Join-Path $dir "run.vbs"

if (-not (Test-Path $vbs)) {
    throw "找不到启动脚本: $vbs"
}

$action = New-ScheduledTaskAction -Execute "wscript.exe" -Argument """$vbs""" -WorkingDirectory $dir

# 一小时后开始，之后每小时一次；开机或休眠错过的任务在恢复后补跑
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(2) `
    -RepetitionInterval (New-TimeSpan -Hours $IntervalHours) `
    -RepetitionDuration (New-TimeSpan -Days 3650)

$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 15) `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -Hidden

$principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" `
    -LogonType Interactive -RunLevel Limited

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger `
    -Settings $settings -Principal $principal -Force `
    -Description "每小时把 ActivityWatch 事件增量备份到 Neon Postgres" | Out-Null

$info = Get-ScheduledTask -TaskName $TaskName
$next = (Get-ScheduledTaskInfo -TaskName $TaskName).NextRunTime
Write-Host "任务已注册: $($info.TaskName)"
Write-Host "状态: $($info.State)，下次运行: $next"
Write-Host "手动触发: Start-ScheduledTask -TaskName '$TaskName'"
