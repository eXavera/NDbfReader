@ECHO off

CALL .\build.cmd
IF %ERRORLEVEL% GTR 0 EXIT /b

.\packages\xunit.runner.console.2.3.1\tools\net452\xunit.console.exe .\NDbfReader.Tests\bin\Debug\NDbfReader.Tests.dll