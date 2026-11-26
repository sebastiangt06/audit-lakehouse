# AWS Glue 5.0 official image: Spark 3.5.4-amzn, Iceberg 1.7.1-amzn-1, Python 3.11, awsglue.
# Anonymous pull from ECR Public (no AWS account). Pinned by tag AND index digest (amd64 + arm64).
# Nothing is downloaded at build or run time: Iceberg, S3FileIO and the Kafka connector are the
# AWS-built jars already in the image (see conf/spark-defaults.lake.conf).
FROM public.ecr.aws/glue/aws-glue-libs:5@sha256:a54bd25fb72c55a2f28d07656a3cda943a042f345cc25f4c2c170667be864f01

USER root

# Our settings are APPENDED to the image's spark-defaults.conf: the image's classpath stays, and
# for repeated keys the last occurrence wins (that is how the Glue Data Catalog metastore is
# switched off). --properties-file is not used because it would replace the whole file.
COPY conf/spark-defaults.lake.conf /tmp/spark-defaults.lake.conf
RUN cat /tmp/spark-defaults.lake.conf >> /etc/spark/conf/spark-defaults.conf \
    && rm /tmp/spark-defaults.lake.conf \
    # A named volume mounted on a new directory inherits its owner: hadoop must own the checkpoints.
    && mkdir -p /checkpoints && chown hadoop:hadoop /checkpoints

COPY --chown=hadoop:hadoop jobs/ /opt/glue/jobs/
COPY --chown=hadoop:hadoop tests/ /opt/glue/tests/

USER hadoop
WORKDIR /opt/glue

# The image's entrypoint is `bash -l`, which would treat `spark-sql` as a script file argument.
# Clearing it lets `docker compose run glue-jobs spark-sql -e "..."` run the command directly.
ENTRYPOINT []
CMD ["bash"]
