@echo off
setlocal EnableDelayedExpansion
set GS_COMMAND_ARGS=%*
call "%~dp0\..\..\bin\xap.bat" run-command scala-repl
endlocal