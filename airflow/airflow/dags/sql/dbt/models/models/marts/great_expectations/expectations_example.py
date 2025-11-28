import great_expectations as ge
import pandas as pd


def run_expectations(path):
df = pd.read_csv(path)
gdf = ge.from_pandas(df)
result = gdf.expect_column_values_to_not_be_null('customer_id')
print(result)
result2 = gdf.expect_column_values_to_be_unique('customer_id')
print(result2)


if __name__ == '__main__':
run_expectations('/tmp/silver/stage_customers.csv')
