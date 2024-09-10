import pytest

from pyspark.sql import Row


@pytest.fixture
def sample_data_to_split_name_column(spark):
    data = [
        Row(PersonID="1", Name="Doe, Abraham, John", Sex="M", Born="01/01/1980"),
        Row(PersonID="2", Name="Smith, Jane", Sex="F", Born="05/15/1985"),
    ]

    return spark.createDataFrame(data)


@pytest.fixture
def sample_data_for_extract_dob(spark):
    data = [
        Row(PersonID="1", Name="Johnson, Bob", Sex="M", Born="4/8/1952 12:00:00 AM"),
        Row(PersonID="2", Name="Johnson, Bob", Sex="O", Born="11/4/2015 12:00:00 AM"),
        Row(PersonID="3", Name="Johnson, Bob", Sex="D", Born="10/31/2000 12:00:00 AM"),
    ]

    return spark.createDataFrame(data)

@pytest.fixture
def sample_data_to_extract_dob(spark):
    data = [
        Row(PersonID="1", Name="Johnson, Bob", Sex="M", Born="04/08/1952 12:00:00 AM"),
        Row(PersonID="2", Name="Johnson, Bob", Sex="O", Born="11/04/2015 12:00:00 AM"),
        Row(PersonID="3", Name="Johnson, Bob", Sex="D", Born="10/31/2000 12:00:00 AM"),
    ]

    return spark.createDataFrame(data)


@pytest.fixture
def sample_data_to_extract_gender(spark):
    data = [
        Row(PersonID="1", Name="Doe, Abraham, John", Sex="M", Born="01/01/1980"),
        Row(PersonID="2", Name="Smith, Jane", Sex="F", Born="05/15/1985"),
        Row(PersonID="3", Name=None, Sex="F", Born="08/21/1990"),
        Row(PersonID="4", Name="Johnson, Bob", Sex="M", Born="12/10/1975"),
        Row(PersonID="5", Name="Johnson, Bob", Sex="O", Born="12/10/1975"),
        Row(PersonID="6", Name="Johnson, Bob", Sex="D", Born="12/10/1975"),
        Row(PersonID="7", Name="Johnson, Bob", Sex="unknown", Born="12/10/1975")
    ]

    return spark.createDataFrame(data)

@pytest.fixture
def sample_demographics(spark):
    data = [
        Row(PersonID="1", Name="Doe, John", Sex="M", Born="01/01/1980"),
        Row(PersonID="2", Name="Smith, Jane", Sex="F", Born="05/15/1985"),
        Row(PersonID="3", Name=None, Sex="O", Born="08/21/1990"),
        Row(PersonID=None, Name="Johnson, Bob", Sex="M", Born="12/10/1975")
    ]
    return spark.createDataFrame(data)

@pytest.fixture
def sample_conditions(spark):
    data = [
        Row(PersonID="1", Dx_Code="Z01.818"),
        Row(PersonID="1", Dx_Code="H35.363"),
        Row(PersonID="2", Dx_Code="E11.9"),
        Row(PersonID="2", Dx_Code="I10")
    ]
    return spark.createDataFrame(data)

@pytest.fixture
def sample_measures(spark):
    data = [
        Row(PersonID="1", MeasureName="Measure_1", Adherent=0),
        Row(PersonID="1", MeasureName="Measure_2", Adherent=1),
        Row(PersonID="2", MeasureName="Measure_3", Adherent=0),
        Row(PersonID="2", MeasureName="Measure_4", Adherent=1)
    ]
    return spark.createDataFrame(data)

@pytest.fixture
def sample_attributions(spark):
    data = [
        Row(PersonID="1", Last_Visit__dt="07/01/2024 12:00:00", Next_Visit__dt="09/15/2024 10:00:00", Provider_NPI="NPI_1"),
        Row(PersonID="1", Last_Visit__dt="08/25/2024 12:00:00", Next_Visit__dt="10/15/2024 10:00:00", Provider_NPI="NPI_1"),
        Row(PersonID="2", Last_Visit__dt="07/25/2024 14:30:00", Next_Visit__dt="12/10/2024 09:30:00", Provider_NPI="NPI_2"),
    ]
    return spark.createDataFrame(data)
