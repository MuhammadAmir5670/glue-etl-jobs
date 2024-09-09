import os

from datetime import date
from unittest.mock import patch, MagicMock

from jobs.aggregate_patients_etl import split_name_column, extract_dob, extract_gender, process_patients_demographics
from jobs.aggregate_patients_etl import main, process_conditions, process_attributions, process_measures

def test_split_name_column(sample_data_to_split_name_column):
    df = split_name_column(sample_data_to_split_name_column)
    result = df.select("first_name", "middle_name", "last_name").collect()

    expected = [
        {"first_name": "Doe", "middle_name": "Abraham", "last_name": "John"},
        {"first_name": "Smith", "middle_name": None, "last_name": "Jane"},
    ]
    assert [row.asDict() for row in result] == expected

def test_extract_dob(spark, sample_data_to_extract_dob):
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    df = extract_dob(sample_data_to_extract_dob)
    result = df.select("date_of_birth").collect()
    expected = [
        {"date_of_birth": "1952-04-08"},
        {"date_of_birth": "2015-11-04"},
        {"date_of_birth": "2000-10-31"}
    ]
    assert [row["date_of_birth"].strftime('%Y-%m-%d') for row in result] == [exp["date_of_birth"] for exp in expected]

def test_extract_gender(sample_data_to_extract_gender):
    df = extract_gender(sample_data_to_extract_gender)
    result = df.select("gender").collect()
    expected = [
        {"gender": "Male"},
        {"gender": "Female"},
        {"gender": "Female"},
        {"gender": "Male"},
        {"gender": "Other"},
        {"gender": "Other"},
        {"gender": ""}
    ]
    assert [row.asDict() for row in result] == expected

def test_process_patients_demographics(sample_demographics):
    df = process_patients_demographics(sample_demographics)
    result = df.select("PersonID", "first_name", "middle_name", "last_name", "date_of_birth", "gender").collect()

    expected = [
        {"PersonID": "1", "first_name": "Doe", "middle_name": None, "last_name": "John", "date_of_birth": date(1980, 1, 1), "gender": "Male"},
        {"PersonID": "2", "first_name": "Smith", "middle_name": None, "last_name": "Jane", "date_of_birth": date(1985, 5, 15), "gender": "Female"},
        {"PersonID": "3", "first_name": None, "middle_name": None, "last_name": None, "date_of_birth": date(1990, 8, 21), "gender": "Other"}
    ]
    assert [row.asDict() for row in result] == expected

def test_process_conditions(sample_conditions):
    df = process_conditions(sample_conditions)
    result = df.select("PersonID", "problem_list").collect()

    expected = [
        {"PersonID": "1", "problem_list": "Z01.818 | H35.363"},
        {"PersonID": "2", "problem_list": "E11.9 | I10"}
    ]
    assert [row.asDict() for row in result] == expected

def test_process_attributions(spark, sample_attributions):
    df = process_attributions(sample_attributions)
    result = df.select("PersonID", "last_pcp_visit_date", "insurance_pcp_npi", "next_pcp_visit_date").collect()

    expected = [
        {"PersonID": "1", "last_pcp_visit_date": date(2024, 8, 25), "insurance_pcp_npi": "NPI_1", "next_pcp_visit_date": date(2024, 9, 15)},
        {"PersonID": "2", "last_pcp_visit_date": date(2024, 7, 25), "insurance_pcp_npi": "NPI_2", "next_pcp_visit_date": date(2024, 12, 10)}
    ]

    assert [row.asDict() for row in result] == expected

def test_process_measures(sample_measures):
    df = process_measures(sample_measures)
    result = df.select("PersonID", "care_gaps_list", "deferred_care_gaps_list").collect()

    expected = [
        {"PersonID": "1", "care_gaps_list": "Measure_1", "deferred_care_gaps_list": "Measure_2"},
        {"PersonID": "2", "care_gaps_list": "Measure_3", "deferred_care_gaps_list": "Measure_4"}
    ]
    assert [row.asDict() for row in result] == expected


@patch('jobs.aggregate_patients_etl.read_patients_dataset')
def test_main_etl_job(read_patients_dataset, sample_demographics, sample_conditions, sample_measures, sample_attributions):
    read_patients_dataset.return_value = (sample_demographics, sample_measures, sample_conditions, sample_attributions)

    final_df = main(return_df=True)

    result = final_df.collect()

    expected_data = [
        {'PersonID': '1', 'first_name': 'Doe', 'middle_name': None, 'last_name': 'John', 'date_of_birth': date(1980, 1, 1), 'gender': 'Male', 'problem_list': 'Z01.818 | H35.363', 'last_pcp_visit_date': date(2024, 8, 25), 'insurance_pcp_npi': 'NPI_1', 'next_pcp_visit_date': date(2024, 9, 15), 'care_gaps_list': 'Measure_1', 'deferred_care_gaps_list': 'Measure_2'},
        {'PersonID': '2', 'first_name': 'Smith', 'middle_name': None, 'last_name': 'Jane', 'date_of_birth': date(1985, 5, 15), 'gender': 'Female', 'problem_list': 'E11.9 | I10', 'last_pcp_visit_date': date(2024, 7, 25), 'insurance_pcp_npi': 'NPI_2', 'next_pcp_visit_date': date(2024, 12, 10), 'care_gaps_list': 'Measure_3', 'deferred_care_gaps_list': 'Measure_4'},
        {'PersonID': '3', 'first_name': None, 'middle_name': None, 'last_name': None, 'date_of_birth': date(1990, 8, 21), 'gender': 'Other', 'problem_list': None, 'last_pcp_visit_date': None, 'insurance_pcp_npi': None, 'next_pcp_visit_date': None, 'care_gaps_list': None, 'deferred_care_gaps_list': None}
    ]

    assert [row.asDict() for row in result] == expected_data
