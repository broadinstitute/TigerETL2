cd %~dp0
set ETLCLASSPATH=c:\AnalyticsETL\TigerETL-assembly-1.0.jar
"C:\scala-2.11.7\bin\scala" -Dconfig.file="c:\\AnalyticsETL\\application.conf" -classpath %ETLCLASSPATH% %1 %2 %3 %4 %5 %6 %7 %8 %9
