FROM 10.254.207.82:8001/vlp/spark:3.5.1-scala2.12-java11

USER root
RUN mkdir /opt/spark/vlp-provisioning
RUN mkdir /opt/spark/vlp-provisioning/jars
RUN mkdir /opt/spark/vlp-provisioning/configs
RUN mkdir /opt/spark/vlp-provisioning/deps

COPY ../target/spark-scala-hdfs-1.0-SNAPSHOT.jar /opt/spark/vlp-provisioning/jars/spark-scala-hdfs-1.0-SNAPSHOT.jar
COPY ./dependencies/*.jar /opt/spark/vlp-provisioning/jars/

COPY ./docker/entrypoint.sh /opt/
#ENTRYPOINT [ "/opt/entrypoint.sh" ]