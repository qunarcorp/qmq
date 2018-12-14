@echo off

if not exist "%JAVA_HOME%\bin\java.exe" echo Please set the JAVA_HOME variable in your environment, run server by java 1.8 & EXIT /B 1
set "JAVA=%JAVA_HOME%\bin\java.exe"

set CONFDIR=%~dp0%..\conf

set CLASSPATH=%CONFDIR%
set CLASSPATH=%~dp0..\*;%~dp0..\lib\*;%CLASSPATH%