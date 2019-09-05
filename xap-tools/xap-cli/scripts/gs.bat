@echo off
setlocal EnableDelayedExpansion
call "%~dp0\setenv.bat"
FOR /F "tokens=*" %%i IN ('"%JAVACMD% -cp "%GS_HOME%\lib\required\*"" com.gigaspaces.start.GsCommandFactory cli') DO set GS_COMMAND=%%i %*
if "!GS_COMMAND:~0,6!"=="Error:" (
  echo %GS_COMMAND%
) else (
  if "%GS_VERBOSE%"=="true" (
    echo Executing GigaSpaces command:
    echo %GS_COMMAND%
    echo --------------------------------------------------------------------------------
  )
  %GS_COMMAND%
)
endlocal