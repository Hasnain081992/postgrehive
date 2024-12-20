import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote

csv_file_path = r"C://Users//44754//Downloads//sales.csv"
df = pd.read_csv(csv_file_path)
print(df)

# transfering file from local to postgresql server@ testdb

password = quote('WelcomeItc@2022')
engine = create_engine('postgresql://consultants:' + password + '@18.132.73.146:5432/testdb')
print('connected')
df.to_sql('sales.csv',engine,index = False)
print('loaded')
