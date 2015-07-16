@ECHO off

CALL .\build.cmd
IF %ERRORLEVEL% GTR 0 EXIT /b

RMDIR ".\opencover" /s /q

MKDIR ".\opencover"
.\packages\OpenCover.4.5.3723\OpenCover.Console.exe^
 -register:user^
 -target:".\packages\xunit.runner.console.2.0.0\tools\xunit.console.exe"^
 -targetargs:".\NDbfReader.Tests\bin\Debug\NDbfReader.Tests.dll -noshadow -quiet"^
 -filter:+[NDbfReader]*^
 -output:.\opencover\output.xml

 MKDIR ".\opencover\report" /s /q
.\packages\ReportGenerator.2.1.8.0\tools\ReportGenerator.exe^
 -reports:.\opencover\output.xml^
 -targetdir:.\opencover\report^
 -reporttypes:Html
 .\opencover\report\index.htm