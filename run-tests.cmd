@ECHO off

CALL .\build.cmd
IF %ERRORLEVEL% GTR 0 EXIT /b

.\packages\xunit.runners.1.9.2\tools\xunit.console.clr4.exe .\NDbfReader.Tests\bin\Debug\NDbfReader.Tests.dll