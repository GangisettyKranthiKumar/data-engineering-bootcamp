import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

def load_data():
    source_df=pd.read_csv(os.path.join(DATA_DIR, 'source_data.csv'))
    target_df=pd.read_csv(os.path.join(DATA_DIR, 'target_data.csv'))
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
    
def read_control_table():
    control_df = pd.read_csv(os.path.join(DATA_DIR, "control_table.csv"))  
    return control_df.loc[0, 'last_run_date']

def get_incremental_load(source_df, last_run_date):
    source_df['created_date'] = pd.to_datetime(source_df['created_date'])
    last_run_date=pd.to_datetime(last_run_date)
    incremental_df = source_df[source_df['created_date'] > last_run_date]
    return incremental_df

def main():
    source_df, target_df = load_data()

    last_run_date = read_control_table()
    incremental_source = get_incremental_load(source_df, last_run_date)

    print("Incremental Load Data (raw):")
    print(incremental_source)

    if incremental_source.empty:
        print("No new data to process in incremental load.")
        return

    # Deduplicate incremental data
    incremental_source = (
        incremental_source
        .sort_values("created_date")
        .drop_duplicates(subset=["id"], keep="last")
    )

    # Validate incremental data
    validate_primary_key(incremental_source, "id", "incremental_source")

    # 🔴 THIS IS THE NEW STEP (SIMULATED LOAD)
    updated_target_df = pd.concat(
        [target_df, incremental_source],
        ignore_index=True
    )

    # Reconcile AFTER load
    validate_source_target_reconciliation(
        incremental_source,
        updated_target_df,
        "id"
    )

    # Update control table
    new_last_run_date = incremental_source["created_date"].max().strftime("%Y-%m-%d")
    pd.DataFrame(
        [new_last_run_date],
        columns=["last_run_date"]
    ).to_csv(os.path.join(DATA_DIR, "control_table.csv"), index=False)

    print("Incremental load completed successfully")

if __name__ == "__main__":
    main()
