import sys, getopt
import pandas as pd

def sql_insert(source, target):
    sql_texts = []
    for index, row in source.iterrows():       
        sql_texts.append('INSERT INTO '+target+' ('+ str(', '.join(source.columns))+ ') VALUES '+ str(tuple(row.values)))        
    return sql_texts

def print_json(devices):
    print(devices.to_json(orient='records',  indent=4))

def print_sql(devices):
    sql_text = SQL_INSERT_STATEMENT_FROM_DATAFRAME(devices, 'devices')
    sql_text = '\n\n'.join(sql_text)
    print(sql_text.encode('utf8'))

def print_summary(devices):
   brand = devices.groupby('device_brand_name').agg(
               popular_model=('device_model_name', lambda x: x.mode()), 
               least_popular_model=('device_model_name',  lambda x: x.value_counts().index[-1]),
               count=('device_model_name', 'count'),
               popular_region=('geo_region', lambda x: x.mode()),
               popular_device_language=('device_language', lambda x: x.mode()),
               popular_device_category=('device_category', lambda x: x.mode())
               ).sort_values('count', ascending=False)[0:10]
      
   device_cat = devices.groupby('device_category').agg(
              popular_brand=('device_brand_name', lambda x: x[x != ''].mode()), 
               least_popular_brand=('device_brand_name',  lambda x: x[x !=''].value_counts().index[-1]),
              popular_model=('device_model_name', lambda x: x[x != ''].mode()), 
               least_popular_model=('device_model_name',  lambda x: x[x != ''].value_counts().index[-1]),
                popular_region=('geo_region', lambda x: x[x != ''].mode()), 
               least_popular_region=('geo_region',  lambda x: x[x != ''].value_counts().index[-1]),
                max_app_version=('app_version', 'max'), 
                min_app_version=('app_version', 'min'), 
               count=('device_model_name', 'count')
               ).sort_values('count', ascending=False)

   device_model = devices.loc[(devices.device_model_name != "")].groupby(['device_model_name', 'device_brand_name', 'device_category']).agg(
               popular_region=('geo_region', lambda x: x.mode()), 
               least_popular_region=('geo_region',  lambda x: x.value_counts().index[-1]),
               count=('device_model_name', 'count')).sort_values('count', ascending=False)[0:10]

   device_region = devices.loc[(devices.geo_region != "")].groupby('geo_region').agg(
              popular_brand=('device_brand_name', lambda x: x.mode()), 
               least_popular_brand=('device_brand_name',  lambda x: x.value_counts().index[-1]),
              popular_model=('device_model_name', lambda x: x.mode()), 
               least_popular_model=('device_model_name',  lambda x: x.value_counts().index[-1]),
               iOS=('device_os', lambda x: (x.str.upper() == 'IOS').sum()),
               android=('device_os', lambda x: (x.str.upper() == 'ANDROID').sum()),
               count=('geo_region', 'count')
            ).sort_values('count', ascending=False)[0:10]

   print(brand.to_string())    
   print(device_cat.to_string())   
   print(device_model.to_string())
   print(device_region.to_string())


def read_csv(filename, jsonFormat, summary, sql):
   devices = pd.read_csv(filename)
   devices = devices.fillna('')
   if jsonFormat:
     print_json(devices)
   elif sql:
     print_sql(devices)
   elif summary:
      print_summary(devices)
   else:
     print(devices.to_string().encode('utf8'))

def main(argv):
   inputfile = ''
   jsonFormat = False
   summary = False
   sql = False
   try:
      opts, args = getopt.getopt(argv,"hi:jssql",["ifile=", "json", "summary", "sql"])
   except getopt.GetoptError:
      print('main.py -i <inputfile>')
   for opt, arg in opts:
      if opt == '-h':
         print('test.py -i <inputfile> --json --summary --sql')
         sys.exit()
      elif opt in ("-i", "--ifile"):
         inputfile = arg
      elif opt in ("-j", "--json"):
         jsonFormat = True
      elif opt in ("-s", "--summary"):
         summary = True
      elif opt in ("-sql", "--sql"):
         sql = True

   read_csv(inputfile, jsonFormat, summary, sql)

if __name__ == '__main__':
    main(sys.argv[1:])
