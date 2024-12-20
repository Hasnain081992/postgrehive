import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote


csv_file_path = r"C:\Users\44754\OneDrive\Documents\vehicle_sales_update.csv"
df = pd.read_csv(csv_file_path)
print(df)

password = quote('WelcomeItc@2022')

engine = create_engine('postgresql://consultants:' + password + '@18.132.73.146:5432/testdb')
print('connected')
df.to_sql('vechile_sales', engine, if_exists='append', index=False)
print('loaded')

