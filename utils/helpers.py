from pyspark.sql import functions as F
from pyspark.sql import DataFrame


def filter_out_missing_values(df: DataFrame, column: str) -> DataFrame:
    """
    This function removes rows where the specified column is either:
    - Null
    - An empty string (after trimming any leading or trailing whitespace)

    Parameters:
    - df: DataFrame
        The input PySpark DataFrame to be filtered.
    - column: str
        The name of the column to check for missing values.

    Returns:
    - DataFrame
        A new DataFrame with rows removed where the specified column has missing values.
    """
    return df.filter(F.col(column).isNotNull() | (F.trim(F.col(column)) != ""))


def map_column_with_default(
    df: DataFrame,
    source_col: str,
    target_col: str,
    mapping: dict,
    default_value: str
) -> DataFrame:
    """
    Dynamically map values from one column to another based on a provided dictionary,
    with a default value for any unmatched entries.

    Parameters:
    - df: The input DataFrame.
    - source_col: The column to map values from.
    - target_col: The column to store the mapped values.
    - mapping: A dictionary where keys are the values to match in source_col, and values are the mapped results.

    Returns:
    - DataFrame with the new mapped column.
    """
    # Start with the default value
    col_expr = F.lit(default_value)

    # Apply the mapping using when() conditions
    for key, value in mapping.items():
        col_expr = F.when(F.col(source_col) == key, value).otherwise(col_expr)

    # Add the new column to the DataFrame
    return df.withColumn(target_col, col_expr)


def group_by_concat(
    df: DataFrame,
    group_col: str,
    target_col: str,
    source_col: str,
    delimiter: str = " | ",
) -> DataFrame:
    """
    Dynamically concatenate values in a column into a single string, grouped by another column.

    Parameters:
    - df: The input DataFrame.
    - group_col: The column to group by.
    - target_col: The column to store the concatenated results.
    - source_col: The column whose values will be concatenated.
    - delimiter: The delimiter to use between concatenated values (default is " | ").


    Returns:
    - DataFrame with the new target column containing concatenated strings.
    """
    # Perform the concatenation
    df_agg = df.groupBy(group_col).agg(
        F.concat_ws(delimiter, F.collect_list(F.col(source_col))).alias(target_col)
    )

    return df_agg
