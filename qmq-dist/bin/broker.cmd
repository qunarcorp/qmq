@echo off

setlocal
call "%~dp0base.cmd"

set MAIN=qunar.tc.qmq.container.Bootstrap

echo on
java -cp "%CLASSPATH%" "%MAIN%" %*

endlocal