@echo off
setlocal EnableDelayedExpansion
set GS_COMMAND_ID=scala-repl
call "%~dp0\..\..\bin\gs" %*
endlocal