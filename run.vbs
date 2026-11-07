' 以隐藏窗口方式运行一次增量同步，供任务计划程序调用
Option Explicit
Dim shell, fso, dir, nodeExe, cmd
Set shell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")
dir = fso.GetParentFolderName(WScript.ScriptFullName)
nodeExe = "C:\nvm4w\nodejs\node.exe"
If Not fso.FileExists(nodeExe) Then nodeExe = "node.exe"
shell.CurrentDirectory = dir
cmd = """" & nodeExe & """ """ & dir & "\sync.mjs"" --quiet"
shell.Run cmd, 0, False
