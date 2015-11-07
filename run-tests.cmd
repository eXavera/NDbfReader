@ECHO off

CALL .\build.cmd
IF %ERRORLEVEL% GTR 0 EXIT /b

.\packages\xunit.runner.console.2.1.0\tools\xunit.console.exe .\NDbfReader.Tests\bin\Debug\NDbfReader.Tests.dll