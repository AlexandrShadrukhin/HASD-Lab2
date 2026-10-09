FROM apache/spark:3.5.8-scala2.12-java17-python3-ubuntu
USER root
COPY build/dependencies/ /opt/spark/jars/
COPY build/libs/hasd-lab2.jar /opt/lab/hasd-lab2.jar
COPY docker/log4j2.properties /opt/spark/conf/log4j2.properties
ENV JAVA_TOOL_OPTIONS="-Dfile.encoding=UTF-8"
WORKDIR /workspace
USER 185
