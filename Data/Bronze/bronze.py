import json
import pandas as pd
import os
from datetime import datetime


def load_json(file_path):
    # Load raw JSON file from Bronze layer
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            data = json.load(file)
        print("JSON successfully loaded")
        return data
    except Exception as error:
        print("Error loading JSON:", error)
        return None


def create_dataframe(data):
    # Convert JSON data to pandas DataFrame
    if isinstance(data, dict) and "issues" in data:
        df = pd.json_normalize(data["issues"])
    elif isinstance(data, list):
        df = pd.json_normalize(data)
    else:
        df = pd.json_normalize(data)

    print(f"DataFrame created | Rows: {df.shape[0]} | Columns: {df.shape[1]}")
    return df


def save_parquet(df, output_path):
    # Save DataFrame as Parquet file in Bronze layer
    try:
        df.to_parquet(output_path, index=False)
        print("File successfully saved in Bronze layer")
        print("Location:", output_path)
    except Exception as error:
        print("Error saving Parquet file:", error)


def main():

    # Start ingestion pipeline to Bronze layer
    print("==========================================")
    print("INGESTION PIPELINE → BRONZE")
    print("Start time:", datetime.now())
    print("==========================================")

    # Define paths
    bronze_json_path = r"D:\Arquivos Projeto Python\Projeto 01 trilha de Engenharia\Projeto Fast Track\Data\Bronze\jira_issues_raw.json"
    bronze_folder_path = r"D:\Arquivos Projeto Python\Projeto 01 trilha de Engenharia\Projeto Fast Track\Data\Bronze"

    # Ensure Bronze directory exists
    os.makedirs(bronze_folder_path, exist_ok=True)

    # Define Bronze parquet output path
    bronze_parquet_path = os.path.join(bronze_folder_path, "jira_issues_raw.parquet")

    # Process
    data = load_json(bronze_json_path)

    if data is not None:
        df = create_dataframe(data)

        # Copy raw structured data (no transformation applied)
        output_dataframe = df.copy()

        save_parquet(output_dataframe, bronze_parquet_path)

 # Main execution
if __name__ == "__main__":
    main()