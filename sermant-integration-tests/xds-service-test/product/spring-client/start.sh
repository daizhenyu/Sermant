exec java -javaagent:/home/agent/sermant-agent.jar -jar -Dsermant_log_dir=/logs/xds-service
/home/spring-client.jar > /logs/xds-service/spring-client.log 2>&1
