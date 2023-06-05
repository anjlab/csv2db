FROM amazoncorretto:17-al2023-jdk as builder

RUN yum update -y && yum install -y \
  git

RUN mkdir /tmp/csv2db
WORKDIR /tmp/csv2db
COPY . .
RUN ./gradlew clean build


FROM amazoncorretto:17-al2023-jdk

RUN mkdir -p /usr/share/csv2db
COPY --from=builder /tmp/csv2db/build/libs/* /usr/share/csv2db/

ENTRYPOINT ["/usr/share/csv2db/run.sh"]

CMD []%
