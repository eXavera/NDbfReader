@ECHO off

CALL .\build.cmd
IF %ERRORLEVEL% GTR 0 EXIT /b

RMDIR ".\opencover" /s /q

MKDIR ".\opencover"
.\packages\OpenCover.4.6.166\tools\OpenCover.Console.exe^
 -register:user^
 -target:".\packages\xunit.runner.console.2.3.1\tools\net452\xunit.console.exe"^
 -targetargs:".\NDbfReader.Tests\bin\Debug\NDbfReader.Tests.dll -noshadow -quiet"^
 -filter:+[NDbfReader]*^
 -output:.\opencover\output.xml

 MKDIR ".\opencover\report" /s /q
.\packages\ReportGenerator.2.3.5.0\tools\ReportGenerator.exe^
 -reports:.\opencover\output.xml^
 -targetdir:.\opencover\report^
 -reporttypes:Html
 .\opencover\report\index.htm