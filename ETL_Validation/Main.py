import pandas as pd

# Load source data & target data
source_data = pd.read_csv('data/source_data.csv')
target_data = pd.read_csv('data/target_data.csv')

print("Source Data:",source_data.shape)
print("Target Data:",target_data.shape)

#1. Null Check

print("\nNull Check in (Source Data):")
print(source_data.isnull().sum())

#2. Duplicate Check
print("\nDuplicate rows in (Source Data):", source_data.duplicated().sum())

#Negative Value Check

invalid_amounts = source_data[source_data['amount'] <= 0]
print("\nNegative Amounts in (Source Data):", invalid_amounts.shape[0]) 

#4. Reconciliation Check
missing_in_target = set(source_data['id']) - set(target_data['id']) 
print("\nRecords in Source but missing in Target:", missing_in_target)



