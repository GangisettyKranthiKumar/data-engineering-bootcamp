import pandas as pd

def load_data():
    source_df=pd.read_csv('data/source_data.csv')
    target_df=pd.read_csv('data/target_data.csv')
    return source_df, target_df

def validate_row_count(source_df, target_df):
   if target_df.shape[0] > source_df.shape[0]:
         raise ValueError("Row count validation failed: Target has more rows than Source -Impossible Scenario")

def validate_primary_key(df,pk_column,table_name):
    duplicates=df[df.duplicated(subset=[pk_column])]
    if not duplicates.empty:
        raise ValueError(f"Primary Key validation failed: Duplicates found in {table_name} for column {pk_column}")
    
def validate_source_target_reconciliation(source_df, target_df, pk_column):
    missing_ids = set(source_df[pk_column]) - set(target_df[pk_column])
    if missing_ids:
        raise Exception(f"Source-Target Reconciliation failed: Missing IDs in Target - {missing_ids}")
    
def main():
    source_df, target_df = load_data()
    
    # Row Count Validation
    validate_row_count(source_df, target_df)
    
    # Primary Key Validation
    validate_primary_key(source_df, 'id', 'Source Table')
    validate_primary_key(target_df, 'id', 'Target Table')
    
    # Source-Target Reconciliation
    validate_source_target_reconciliation(source_df, target_df, 'id')
    
    print("All validations passed successfully.")

if __name__ == "__main__":
    main()
