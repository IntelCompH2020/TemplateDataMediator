####################################### Build stage #######################################
FROM maven:3.6.3-openjdk-11-slim AS build-stage

COPY JavaTrivialsToolkit /build/JavaTrivialsToolkit/
COPY SparkSQLComposer /build/SparkSQLComposer/
COPY InfraHelperLib /build/InfraHelperLib/
COPY TemplateDataMediator /build/TemplateDataMediator/

#Build JavaTrivialsToolkit
WORKDIR /build/JavaTrivialsToolkit/
RUN mvn clean install -DskipTests

#Build InfraHelperLib
WORKDIR /build/InfraHelperLib/
RUN mvn clean install -DskipTests

#Build SparkSQLComposer
WORKDIR /build/SparkSQLComposer/
RUN mvn clean install -DskipTests

#Build TemplateDataMediator
WORKDIR /build/TemplateDataMediator/
RUN mvn clean package -Drevision=${BUILD_VERSION} -DskipTests

ARG CREPO_BINARIES_REPO_URL
ARG CREPO_BINARIES_CREDENTIAL
ARG BUILD_VERSION
ENV CREPO_BINARIES_REPO_URL=$CREPO_BINARIES_REPO_URL
ENV CREPO_BINARIES_CREDENTIAL=$CREPO_BINARIES_CREDENTIAL
ENV BUILD_VERSION=$BUILD_VERSION

RUN curl --location --request PUT "${CREPO_BINARIES_REPO_URL}intelcomp/mediators/TemplateDataMediator/TemplateDataMediator-${BUILD_VERSION}.jar" \
--header "Authorization: Basic ${CREPO_BINARIES_CREDENTIAL}" \
--header "Content-Type: application/json" \
--data-binary "@/build/TemplateDataMediator/target/SparkSqlTemplateDataMediator-${BUILD_VERSION}-jar-with-dependencies.jar"