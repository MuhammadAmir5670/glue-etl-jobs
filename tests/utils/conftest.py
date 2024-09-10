import pytest
from pyspark.sql import Row


@pytest.fixture
def sample_data_with_missing_values(spark):
    """Fixture to create a DataFrame with some missing values."""
    data = [
        Row(PersonID="1", Name="John Doe", Sex="M", Born="01/01/1980"),
        Row(PersonID="2", Name=None, Sex="F", Born="05/15/1985"),
        Row(PersonID="3", Name="Alice Smith", Sex="", Born="08/21/1990"),
        Row(PersonID=None, Name="Bob Johnson", Sex="M", Born="12/10/1975")
    ]
    return spark.createDataFrame(data)

@pytest.fixture
def sample_data_for_mapping(spark):
    """Fixture to create a DataFrame for testing column mapping."""
    data = [
        Row(PersonID="1", Sex="M"),
        Row(PersonID="2", Sex="F"),
        Row(PersonID="3", Sex="unknown"),
        Row(PersonID="4", Sex="N/A")
    ]
    return spark.createDataFrame(data)

@pytest.fixture
def sample_data_for_concat(spark):
    """Fixture to create a DataFrame for testing group_by_concat."""
    data = [
        Row(PersonID="1", Dx_Code="Z01.818"),
        Row(PersonID="1", Dx_Code="H35.363"),
        Row(PersonID="2", Dx_Code="E11.9"),
        Row(PersonID="2", Dx_Code="I10")
    ]
    return spark.createDataFrame(data)
