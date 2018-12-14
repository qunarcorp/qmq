@echo off

setlocal
call "%~dp0base.cmd"

set MAIN=qunar.tc.qmq.meta.startup.Bootstrap

echo on
"%JAVA%" -cp "%CLASSPATH%" "%MAIN%" %*

endlocal