FROM mozilla/sbt

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
COPY container .

RUN ["sbt", "compile"]

ENTRYPOINT ["sbt", "run"]
