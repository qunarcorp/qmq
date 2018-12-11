@echo off

setlocal
call "%~dp0base.cmd"

set MAIN=qunar.tc.qmq.tools.Tools

echo on
java -cp "%CLASSPATH%" "%MAIN%" %*

endlocal