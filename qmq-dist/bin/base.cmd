@echo off

set CONFDIR=%~dp0%..\conf

set CLASSPATH=%CONFDIR%
set CLASSPATH=%~dp0..\*;%~dp0..\lib\*;%CLASSPATH%