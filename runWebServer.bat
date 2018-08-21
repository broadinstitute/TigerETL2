cd %~dp0
:start
copy c:\temp\tigeretl-assembly-1.0.jar .\target\scala-2.11\tigeretl-assembly-1.0.jar
call "C:\scala-2.11.7\bin\scala" -Ddummy=WebServer -Dconfig.file=application.conf -classpath .\target\scala-2.11\tigeretl-assembly-1.0.jar analytics.tiger.api.Boot
echo "Restarting ..."
goto :start
