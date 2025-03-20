import sys

from datetime import date
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext


class ArgumentService:
    """
    Service for handling and retrieving AWS Glue job arguments.

    This class initializes and stores the job arguments such as the job name and output path.
    This class can be extended based on the needs of a particular job
    """
    def __init__(self):
        self._set_args()

    def _set_args(self):
        self._args = getResolvedOptions(sys.argv, ['JOB_NAME', 'output_path'])

    @property
    def args(self):
        return self._args

    @property
    def job_name(self):
        return self.args['JOB_NAME']

    @property
    def output_path(self):
        return self.args['output_path']


class ContextService:
    """
    Service for initializing and managing the AWS Glue and Spark contexts.
    """
    def __init__(self, argument_service: ArgumentService):
        sc = SparkContext()
        self.__argument_service = argument_service
        self.__glue_context: GlueContext = GlueContext(sc)
        self.__spark = self.__glue_context.spark_session
        self.__job = Job(self.__glue_context)
        self.__job.init(self.__argument_service.job_name, self.__argument_service.args)

    @property
    def glue_context(self):
        return self.__glue_context

    @property
    def spark(self):
        return self.__spark

    @property
    def argument_service(self):
        return self.__argument_service

    def commit_job(self):
        return self.__job.commit()


def read_from_glue_table(database: str, table: str, glue_context: GlueContext, date) -> DataFrame:
    """
    Reads data from an AWS Glue table for a specific date using a push-down predicate.

    Parameters:
    - database: str
        The name of the database in the Glue Data Catalog.
    - table: str
        The name of the table in the Glue Data Catalog.
    - glueContext: GlueContext
        The GlueContext object used to interact with AWS Glue.
    - date: datetime
        The date object used to extract the year, month, and day for filtering the data.

    Returns:
    - DataFrame
        A PySpark DataFrame containing the filtered data from the Glue table.
    """
    dynamic_frame = glue_context.create_dynamic_frame.from_catalog(
        database=database,
        table_name=table,
        push_down_predicate=f"year='{date.year}' and month='{date.month}' and day='{date.day}'"
    )

    return dynamic_frame.toDF()



def write_to_s3(df: DataFrame, glue_context: GlueContext, output_path: str, date: date) -> None:
    """
    Writes a PySpark DataFrame to an S3 bucket as a CSV file, partitioned by year, month, and day.

    This function takes a DataFrame and adds partition columns ('year', 'month', 'day') based on the provided date.
    It then converts the DataFrame to a Glue DynamicFrame and writes it to the specified S3 output path, partitioned by
    the date columns.

    Parameters:
    - df: DataFrame
        The input PySpark DataFrame to be written to S3.
    - glue_context: GlueContext
        The GlueContext object used to manage the Glue environment and operations.
    - output_path: str
        The S3 path where the output CSV files will be stored.
    - date: date
        The date object used to create the 'year', 'month', and 'day' partition columns.

    Returns:
    - None
    """
    with_partitions = df \
        .withColumn("year", lit(date.year)) \
        .withColumn("month", lit(date.month)) \
        .withColumn("day", lit(date.day)) \

    dynamic_frame = DynamicFrame.fromDF(with_partitions, glue_context, "dynamic_frame")

    glue_context.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="s3",
        connection_options={
            "path": output_path,
            "partitionKeys": ["year", "month", "day"]
        },
        format="csv",
        format_options={"writeHeader": True}
    )
