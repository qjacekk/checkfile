@echo off
:: place this script and file-test-*-jar-with-dependencies.jar in one directory
:: that is on the windows PATH
set JAR=checkfile-1.0-SNAPSHOT-jar-with-dependencies.jar

:: Determine the path to the script's directory
set APPATH=%~DP0

:: Run the application
java -jar %APPATH%%JAR% %*