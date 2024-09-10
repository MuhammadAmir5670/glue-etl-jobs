import sys

from datetime import date
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

from awsglue.context import GlueContext
from utils.glue_helpers import ArgumentService, ContextService, read_from_glue_table, write_to_s3
from utils.helpers import filter_out_missing_values, map_column_with_default, group_by_concat


def read_patients_dataset(glue_context: GlueContext, date: date):
    """
    Reads patient datasets from aws glue catalog table files and returns them as DataFrames.
    """
    patients_demographics = read_from_glue_table("patients", "demographics", glue_context, date)
    patients_measures = read_from_glue_table("patients", "measures", glue_context, date)
    patients_conditions = read_from_glue_table("patients", "conditions", glue_context, date)
    patients_attributions = read_from_glue_table("patients", "attributions", glue_context, date)

    return patients_demographics, \
            patients_measures, \
            patients_conditions, \
            patients_attributions,


def split_name_column(df: DataFrame):
    """
    Splits the 'Name' column into 'first_name', 'middle_name', and 'last_name'.

    Parameters:
    - df: DataFrame
        Input DataFrame containing a 'Name' column.

    Returns:
    - DataFrame
        DataFrame with added columns for 'first_name', 'middle_name', and 'last_name'.
    """
    split_name = F.split(F.col("Name"), ",")
    df = df.withColumn("first_name", split_name.getItem(0))
    df = df.withColumn("middle_name", F.trim(F.when(F.size(split_name) == 3, split_name.getItem(1)).otherwise(None)))
    df = df.withColumn("last_name", F.trim(split_name.getItem(F.size(split_name) - 1)))

    return df

def extract_dob(df: DataFrame):
    """
    Extracts and formats the 'date_of_birth' from the 'Born' column.

    Parameters:
    - df: DataFrame
        Input DataFrame containing a 'Born' column.

    Returns:
    - DataFrame
        DataFrame with a 'date_of_birth' column.
    """
    return df \
        .withColumn("date_of_birth", F.regexp_replace(F.col("Born"), " .*", "")) \
        .withColumn("date_of_birth", F.to_date(F.col("date_of_birth"), "MM/dd/yyyy"))


def extract_gender(df: DataFrame):
    """
    Maps 'Sex' column values to 'gender' using a predefined dictionary.

    Parameters:
    - df: DataFrame
        Input DataFrame containing a 'Sex' column.

    Returns:
    - DataFrame
        DataFrame with a 'gender' column.
    """
    MAPPING = { "F": "Female", "M": "Male", "unknown": "" }
    DEFAULT = "Other"

    return map_column_with_default(df, "Sex", "gender", MAPPING, DEFAULT)

def process_patients_demographics(demographics):
    """
    Processes patient demographics to extract ("PersonID", "first_name", "middle_name", "last_name", "date_of_birth", "gender").
    """
    demographics = filter_out_missing_values(demographics, "PersonID")
    # Get First, Middle and Last Name
    demographics = split_name_column(demographics)
    # Get date_of_birth by Born
    demographics = extract_dob(demographics)
    # Get Gender
    demographics = extract_gender(demographics)

    return demographics.select("PersonID", "first_name", "middle_name", "last_name", "date_of_birth", "gender")

def process_conditions(conditions):
    """
    Processes patient conditions to group and concatenate diagnosis codes into 'problem_list.
    """
    conditions = filter_out_missing_values(conditions, "PersonID")
    conditions = filter_out_missing_values(conditions, "Dx_Code")
    patient_risks = group_by_concat(
        conditions,
        group_col="PersonID",
        source_col="Dx_Code",
        target_col="problem_list",
    )

    return patient_risks

def process_attributions(attributions):
    """
    Processes patient attribution data to extract last visit and next visit dates and insurance NPI information.
    """
    patient_visits = filter_out_missing_values(attributions, "PersonID")

    date_exp = lambda column: F.to_date(F.regexp_replace(F.col(column), " .*", ""), "MM/dd/yyyy")

    patient_visits = patient_visits \
        .withColumn("last_pcp_visit_date", date_exp("Last_Visit__dt")) \
        .withColumn("next_pcp_visit_date", date_exp("Next_Visit__dt"))

    # Group by PersonID to get the most recent last visit
    patient_last_visits = patient_visits.groupBy("PersonID").agg(
        F.max(F.struct("last_pcp_visit_date", "Provider_NPI")).alias("max_last_visit")
    )

    # Extract the most recent last visit date and corresponding Provider_NPI
    patient_last_visits = patient_last_visits.select(
        "PersonID",
        patient_last_visits["max_last_visit.last_pcp_visit_date"].alias("last_pcp_visit_date"),
        patient_last_visits["max_last_visit.Provider_NPI"].alias("insurance_pcp_npi")
    )

    # Group by PersonID to get the earliest next visit
    patient_next_visits = patient_visits.groupBy("PersonID").agg(
        F.min("next_pcp_visit_date").alias("next_pcp_visit_date")
    )

    return patient_last_visits.join(patient_next_visits, on="PersonID", how="outer")

def process_measures(measures):
    """
    Processes patient measures to identify care gaps and deferred care gaps.
    """
    measures = filter_out_missing_values(measures, "PersonID")
    measures = filter_out_missing_values(measures, "MeasureName")

    care_gaps_list = group_by_concat(
        measures.filter(F.col("Adherent") == 0),
        group_col="PersonID",
        source_col="MeasureName",
        target_col="care_gaps_list"
    )

    deferred_care_gaps_list = group_by_concat(
        measures.filter(F.col("Adherent") == 1),
        group_col="PersonID",
        source_col="MeasureName",
        target_col="deferred_care_gaps_list"
    )

    return care_gaps_list.join(deferred_care_gaps_list, on="PersonID")


def process_patients(glue_context, current_date):
    patients_demographics, \
    patients_measures, \
    patients_conditions, \
    patients_attributions = read_patients_dataset(glue_context, current_date)

    patients_demographics = process_patients_demographics(patients_demographics)
    patients_risks = process_conditions(patients_conditions)
    patients_visits = process_attributions(patients_attributions)
    patients_measures = process_measures(patients_measures)

    final_df = patients_demographics.join(patients_risks, on="PersonID", how="left")
    final_df = final_df.join(patients_visits, on="PersonID", how="left")
    final_df = final_df.join(patients_measures, on="PersonID", how="left")
    final_df = final_df.withColumnRenamed("PersonID", "mrn_in_primary_care_practice")

    return final_df

def main():
    argument_service = ArgumentService()
    context_service = ContextService(argument_service)
    glue_context, spark = context_service.glue_context, context_service.spark

    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    current_date = date(2023, 11, 29)
    results = process_patients(glue_context, current_date)

    write_to_s3(results, glue_context, argument_service.output_path, current_date)

    context_service.commit_job()
    spark.stop()
    sys.exit(0)


if __name__ == '__main__':
    main()
