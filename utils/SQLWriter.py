import pandas as pd
import numpy as np
import os, pymysql

class SQLWriter:

    def __init__(
        self,
        table_name = 'my_table',
        create_table_statement = None,
        input_csv_file = 'data.csv',
        output_sql_file = 'output.sql',
        drop_columns = None,
        data_transformer = None
    ):  
        self.csv_file = input_csv_file  # Read CSV file
        self.sql_file = output_sql_file # Define output SQL file
        self.table_name = table_name
        self.create_table_statement = create_table_statement
        self.drop_columns = drop_columns
        self.data_transformer = data_transformer
    
    def _quote_escape(self, value):
        return str(value).replace("'", "''")
    
    def escape_value(self, value):
        return pymysql.converters.escape_string(value)
    
    def data_transform(self, df):
        try:
            df = self.data_transformer(df)
            if df.empty:
                raise ValueError("The transformed DataFrame is empty.")
            return df
        except Exception as e:
            print(f"Error during data transformation: {str(e)}")
            exit(1)

    def get_input(self):

        try:
            df = pd.read_csv(self.csv_file)
            
            if df.empty:
                raise ValueError("The CSV file is empty.")

            # Drop columns if necessary
            if self.drop_columns is not None:
                for col in self.drop_columns:
                    df = df.drop(col, axis=1)

            if self.data_transformer is not None:
                df = self.data_transform(df)

            return df
        
        except FileNotFoundError:
            print(f"Error: The file '{self.csv_file}' was not found.")
            exit(1)
        except pd.errors.ParserError:
            print(f"Error: The file '{self.csv_file}' is not a valid CSV or is corrupted.")
            exit(1)
        except Exception as e:
            print(f"Error reading CSV file: {str(e)}")
            exit(1)

    def _ensure_output_destination_existance(self):
        output_dir = os.path.dirname(self.sql_file)
        if output_dir:  # Check if there's a directory path (not empty)
            try:
                os.makedirs(output_dir, exist_ok=True)  # Create directory if it doesn't exist
            except PermissionError:
                print(f"Error: Permission denied when creating directory '{output_dir}'.")
                exit(1)
            except Exception as e:
                print(f"Error creating directory '{output_dir}': {str(e)}")
                exit(1)
    
    def write_sql(self):

        try:
            df = self.get_input()

            self._ensure_output_destination_existance()
            # Open file to write SQL statements
            with open(self.sql_file, 'w', encoding="utf-8") as f:

                f.write('START TRANSACTION;\n')
                if self.create_table_statement is not None:
                    # Example: Create a table (modify schema as needed)
                    f.write(self.create_table_statement)
                    f.write('\n')

                # Write INSERT statements
                for index, row in df.iterrows():
                    try:
                        # Identify non-null columns and values
                        non_null_mask = ~row.isna()  # Creates a boolean mask where True indicates non-null values
                        non_null_columns = df.columns[non_null_mask].tolist()
                        non_null_values = row[non_null_mask].tolist()

                        if not non_null_columns:
                            print(f"Skipping row {index}: All values are null")
                            continue

                        # Format values for SQL (quote strings, convert others to string)
                        formatted_values = ', '.join(
                            [f"'{self.escape_value(value)}'" if isinstance(value, str) else str(value) 
                            for value in non_null_values]
                        )
                        # Write the INSERT statement with non-null columns and values
                        f.write(f"INSERT INTO {self.table_name} ({', '.join(non_null_columns)}) VALUES ({formatted_values});\n")
                    except Exception as e:
                        print(f"Error processing row {index}: {str(e)}")
                        continue  # Skip problematic rows and continue
                
                f.write('COMMIT;\n')

            print(f"SQL file '{self.sql_file}' generated successfully.")
            
        except PermissionError:
            print(f"Error: Permission denied when writing to '{self.sql_file}'.")
            exit(1)
        except Exception as e:
            print(f"Error writing to SQL file: {str(e)}")
            exit(1)