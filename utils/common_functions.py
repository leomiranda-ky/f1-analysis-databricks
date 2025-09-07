from pyspark.sql.functions import current_timestamp

def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df


def df_column_to_list(input_df, column_name):
    df_row_list = input_df.select(column_name).distinct().collect()

    column_value_list = [row[column_name] for row in df_row_list]
    return column_value_list