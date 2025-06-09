import pandas as pd
import numpy as np

class CSVDataRowsSanitizer:
    def __init__(self, file_path):
        """
        Initialize with the path to the CSV file.
        
        Args:
            file_path (str): Path to the CSV file.
        """
        self.file_path = file_path
        self.df = None
        self.count_report = None

    def load_csv(self):
        """Load the CSV file into a Pandas DataFrame."""
        try:
            self.df = pd.read_csv(self.file_path)
        except FileNotFoundError:
            raise FileNotFoundError(f"CSV file not found at: {self.file_path}")
        except Exception as e:
            raise Exception(f"Error loading CSV: {str(e)}")

    def process_duplicates(self, columns_to_check, action='modify', modify_column=None):
        """
        Process non-unique rows based on specified columns.
        
        Args:
            columns_to_check (list): List of column names to check for uniqueness.
            action (str): Action to take ('modify' to update values, 'remove' to delete duplicates).
            modify_column (str): Column to modify to ensure uniqueness (required if action='modify').
        
        Returns:
            bool: True if duplicates were found and processed, False otherwise.
        """
        if self.df is None:
            raise ValueError("CSV not loaded. Call load_csv() first.")
        
        if not all(col in self.df.columns for col in columns_to_check):
            raise ValueError("One or more specified columns not found in CSV.")
        
        if action not in ['modify', 'remove']:
            raise ValueError("Action must be 'modify' or 'remove'.")
        
        if action == 'modify' and (modify_column is None or modify_column not in self.df.columns):
            raise ValueError("modify_column must be specified and exist in CSV when action='modify'.")

        # Identify duplicate rows based on the combination of specified columns
        duplicates = self.df.duplicated(subset=columns_to_check, keep=False)

        if not duplicates.any():
            print("No duplicates found.")
            return False

        if action == 'remove':
            # Keep only the first occurrence of each combination
            self.df = self.df.drop_duplicates(subset=columns_to_check, keep='first')
            print("Duplicate rows removed.")
        else:
            # Modify duplicates to make combinations unique
            duplicate_rows = self.df[duplicates].copy()
            for index in duplicate_rows.index:
                # Get the values of the columns being checked
                duplicate_values = self.df.loc[index, columns_to_check].to_dict()
                # Find all rows with the same combination
                matching_rows = self.df[(self.df[columns_to_check] == pd.Series(duplicate_values)).all(axis=1)]
                
                # Skip the first occurrence
                if index == matching_rows.index[0]:
                    continue
                
                # Add a suffix to the specified column
                count = len(matching_rows[:matching_rows.index.get_loc(index)+1])
                self.df.loc[index, modify_column] = f"{self.df.loc[index, modify_column]}_{count}"
            print(f"Duplicates modified in column '{modify_column}'.")

        return True
    
    def remove_equal_columns_rows(self, column1, column2):
        """
        Remove rows where two specified columns have equal values.
        
        Args:
            column1 (str): Name of the first column to compare.
            column2 (str): Name of the second column to compare.
        
        Returns:
            bool: True if any rows were removed, False if no rows had equal values or columns not found.
        """
        if self.df is None:
            raise ValueError("CSV not loaded. Call load_csv() first.")
        
        if column1 not in self.df.columns or column2 not in self.df.columns:
            print(f"One or both columns ('{column1}', '{column2}') not found in CSV. No action taken.")
            return False
        
        # Identify rows where column1 and column2 have equal values
        equal_rows = self.df[column1] == self.df[column2]
        
        if not equal_rows.any():
            print(f"No rows found where '{column1}' equals '{column2}'.")
            return False
        
        # Remove rows where the columns have equal values
        num_rows_before = len(self.df)
        self.df = self.df[~equal_rows]
        num_rows_removed = num_rows_before - len(self.df)
        
        print(f"Removed {num_rows_removed} rows where '{column1}' equals '{column2}'.")
        return True
    
    def trim_column_values(self, column_to_trim, max_length):
        """
        Trim values in the specified column to a maximum character length.
        
        Args:
            column_to_trim (str): Column whose values need to be trimmed.
            max_length (int): Maximum allowed character length for values.
        
        Returns:
            bool: True if any values were trimmed, False otherwise.
        """
        if self.df is None:
            raise ValueError("CSV not loaded. Call load_csv() first.")
        
        if column_to_trim not in self.df.columns:
            raise ValueError(f"Column '{column_to_trim}' not found in CSV.")
        
        if not isinstance(max_length, int) or max_length <= 0:
            raise ValueError("max_length must be a positive integer.")
        
        # Track if any values were trimmed
        trimmed = False
        
        # Convert values to string and trim if necessary
        self.df[column_to_trim] = self.df[column_to_trim].astype(str).apply(
            lambda x: x[:max_length] if len(x) > max_length else x
        )
        
        # Check if any values were actually trimmed
        trimmed = any(self.df[column_to_trim].str.len() < self.df[column_to_trim].str.len().max())
        
        if trimmed:
            print(f"Values in column '{column_to_trim}' trimmed to max length {max_length}.")
        else:
            print(f"No values in column '{column_to_trim}' needed trimming.")
        
        return trimmed
    
    def add_random_column(self, column_name, min_value, max_value, is_integer=True):
        """
        Add a new column with random numbers within a specified range.
        
        Args:
            column_name (str): Name of the new column to add.
            min_value (float): Minimum value for random numbers (inclusive).
            max_value (float): Maximum value for random numbers (inclusive for integers, exclusive for floats).
            is_integer (bool): If True, generate random integers; if False, generate random floats.
        
        Returns:
            bool: True if the column was added, False if the column already exists.
        """
        if self.df is None:
            raise ValueError("CSV not loaded. Call load_csv() first.")
        
        if column_name in self.df.columns:
            print(f"Column '{column_name}' already exists. Skipping addition.")
            return False
        
        if not isinstance(min_value, (int, float)) or not isinstance(max_value, (int, float)):
            raise ValueError("min_value and max_value must be numbers.")
        
        if min_value >= max_value:
            raise ValueError("min_value must be less than max_value.")
        
        # Generate random numbers
        if is_integer:
            # For integers, max_value is inclusive
            random_values = np.random.randint(low=min_value, high=max_value + 1, size=len(self.df))
        else:
            # For floats, max_value is exclusive
            random_values = np.random.uniform(low=min_value, high=max_value, size=len(self.df))
        
        # Add the new column to the DataFrame
        self.df[column_name] = random_values
        print(f"Added column '{column_name}' with random values between {min_value} and {max_value}.")
        
        return True
    
    def remove_column(self, column_to_remove):
        """
        Remove a column with the specified name from the DataFrame.
        
        Args:
            column_to_remove (str): Name of the column to remove.
        
        Returns:
            bool: True if the column was removed, False if the column was not found.
        """
        if self.df is None:
            raise ValueError("CSV not loaded.")
        
        if column_to_remove not in self.df.columns:
            print(f"Column '{column_to_remove}' not found in CSV. No action taken.")
            return False
        
        # Remove the column
        self.df = self.df.drop(columns=column_to_remove)
        print(f"Removed column '{column_to_remove}' from CSV.")
        
        return True

    def count_column_value_appearance(self, key, value):
        # print('key : value =>\n', f"{key} : {value}")
        if key in self.count_report:
            if value in self.count_report[key]:
                self.count_report[key][value] += 1
            else:
                self.count_report[key][value] = 0
    
    def process_counter(self, row):
        counter_range = self.count_report.keys()
        for key in counter_range:
            if key in row:
                self.count_column_value_appearance(key=key, value=str(row[key]))
        return self.count_report

    def apply_row_transformation(self, transform_function, columns_to_count_on_transform=None):
        """
        Apply a user-provided transformation function to each row of the DataFrame.
        
        Args:
            transform_function (callable): Function that takes a pandas Series (row) and returns a modified Series.
        
        Returns:
            bool: True if the transformation was applied, False if no changes were made.
        """
        if self.df is None:
            raise ValueError("CSV not loaded. Call load_csv() first.")
        
        if not callable(transform_function):
            raise ValueError("transform_function must be a callable function.")
        
        print('columns_to_count_on_transform => ', columns_to_count_on_transform)

        self.count_report = None
        self.count_report = {}
        if columns_to_count_on_transform is not None:
            for column_key in columns_to_count_on_transform:
                self.count_report[column_key] = {}
        
        try:
            # Apply the transformation function to each row
            original_df = self.df.copy()
            if self.count_report is not None and columns_to_count_on_transform is not None:
                self.df = self.df.apply(lambda row: transform_function(row, self.process_counter), axis=1)
            else:
                self.df = self.df.apply(transform_function, axis=1)
            
            # Check if any changes were made
            changed = not self.df.equals(original_df)
            
            if changed:
                print("Row transformations applied successfully.")
            else:
                print("No changes made by the transformation function.")
            
            return changed
        except Exception as e:
            raise Exception(f"Error applying transformation function: {str(e)}")

    def add_empty_column(self, column_name, extra_rows=0):
        """
        Add a new column with the specified name and fill it with empty values (NaN).
        Optionally append a specified number of new rows with all columns set to NaN.
        
        Args:
            column_name (str): Name of the new column to add.
            extra_rows (int): Number of additional rows to append (default: 0).
        
        Returns:
            bool: True if the column and/or rows were added, False if the column already exists.
        """
        if self.df is None:
            raise ValueError("CSV not loaded. Call load_csv() first.")
        
        if column_name in self.df.columns:
            print(f"Column '{column_name}' already exists. Skipping addition.")
            return False
        
        if not isinstance(extra_rows, int) or extra_rows < 0:
            raise ValueError("extra_rows must be a non-negative integer.")
        
        # Add the new column filled with NaN
        self.df[column_name] = np.nan
        
        # Append extra rows if specified
        if extra_rows > 0:
            # Create a new DataFrame with extra_rows rows and same columns, filled with NaN
            new_rows = pd.DataFrame(np.nan, index=range(extra_rows), columns=self.df.columns)
            # Append the new rows to the DataFrame
            self.df = pd.concat([self.df, new_rows], ignore_index=True)
            print(f"Appended {extra_rows} new rows with NaN values.")
        
        print(f"Added empty column '{column_name}' with NaN values.")
        return True

    def save_csv(self):
        """Overwrite the CSV file with the updated DataFrame."""
        if self.df is None:
            raise ValueError("CSV not loaded. Call load_csv() first.")
        try:
            self.df.to_csv(self.file_path, index=False)
            print(f"CSV file overwritten at: {self.file_path}")
        except Exception as e:
            raise Exception(f"Error saving CSV: {str(e)}")

    def process(self, columns_to_check=None, action='modify', modify_column=None, 
                trim_column=None, max_length=None,
                random_column=None, min_value=None, max_value=None, random_is_integer=True,
                remove_column_name=None, transform_function=None, columns_to_count_on_transform=None,
                empty_column_name=None, empty_extra_rows=0, equal_columns=None):
        """
        Process the CSV: handle duplicates, remove rows with equal column values, trim column values, 
        add random column, remove column, apply row transformation, add empty column with optional extra rows, then save.
        
        Args:
            columns_to_check (list): Columns to check for uniqueness (optional).
            action (str): 'modify' or 'remove' duplicates (used if columns_to_check is provided).
            modify_column (str): Column to modify if action='modify' (used if columns_to_check is provided).
            trim_column (str): Column to trim values (optional).
            max_length (int): Maximum length for values in trim_column (required if trim_column is provided).
            random_column (str): Name of new column for random numbers (optional).
            min_value (float): Minimum value for random numbers (required if random_column is provided).
            max_value (float): Maximum value for random numbers (required if random_column is provided).
            random_is_integer (bool): If True, random numbers are integers; if False, floats.
            remove_column_name (str): Name of column to remove (optional).
            transform_function (callable): Function to transform each row (optional).
            transform_arg (any): Additional argument to pass to transform_function (optional).
            empty_column_name (str): Name of new column to add with empty values (optional).
            empty_extra_rows (int): Number of additional rows to append with NaN values深的 (default: 0).
            equal_columns (tuple): Tuple of two column names to check for equal values and remove rows (optional).
        """
        self.load_csv()
        
        # Process duplicates if columns_to_check is provided
        if columns_to_check:
            self.process_duplicates(columns_to_check, action, modify_column)
        
        # Remove rows with equal values in two columns if equal_columns is provided
        if equal_columns:
            self.remove_equal_columns_rows(equal_columns[0], equal_columns[1])
        
        # Trim column values if trim_column and max_length are provided
        if trim_column and max_length:
            self.trim_column_values(trim_column, max_length)
        
        # Add random column if random_column, min_value, and max_value are provided
        if random_column and min_value is not None and max_value is not None:
            self.add_random_column(random_column, min_value, max_value, random_is_integer)
        
        # Remove column if remove_column_name is provided
        if remove_column_name:
            self.remove_column(remove_column_name)

        # Apply row transformation if transform_function is provided
        if transform_function:
            self.apply_row_transformation(transform_function, columns_to_count_on_transform)
        
        # Add empty column and extra rows if empty_column_name is provided
        if empty_column_name:
            self.add_empty_column(empty_column_name, empty_extra_rows)
        
        self.save_csv()