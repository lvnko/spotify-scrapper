import csv, os, datetime
import random

class CSVWriter:
    def __init__(self, file_path=None, default_data=None):
        """
        Initialize CSVWriter with a default file path and optional default data.
        
        Args:
            file_path (str, optional): Path to the CSV file. Defaults to 'data/sample.csv'.
            default_data (list, optional): Default data to write if none provided. Defaults to [].
        """
        self.file_path = file_path if file_path is not None else "data/sample.csv"
        self.default_data = default_data if default_data is not None else []
        
        # Ensure the directory exists
        output_dir = os.path.dirname(self.file_path)
        if output_dir:
            try:
                os.makedirs(output_dir, exist_ok=True)
            except PermissionError:
                raise PermissionError(f"Permission denied when creating directory '{output_dir}'.")
            except OSError as e:
                raise OSError(f"Error creating directory '{output_dir}': {str(e)}")

    def write(self, data_set=None, mode='w', print_remarks=None):
        """
        Write or append data to a CSV file.
        
        Args:
            data_set (list, optional): List of dictionaries to write. Uses default_data if None.
            mode (str): File mode, 'w' for write (overwrite) or 'a' for append. Defaults to 'w'.
            
        Returns:
            bool: True if successful, False if data_set is empty or invalid.
        """
        # Use provided data or default data
        data_set = data_set if data_set is not None else self.default_data
        
        # Check if data_set is empty or invalid
        if not data_set or len(data_set) <= 0 or not isinstance(data_set, list):
            print("Error: Data set is empty or invalid.")
            return False
        
        # Validate that the first item has keys (for header)
        if not isinstance(data_set[0], dict) or not data_set[0].keys():
            print("Error: Data set must contain dictionaries with valid keys.")
            return False
        
        data_header = data_set[0].keys()
        
        # Get time for logging
        action_time = datetime.datetime.now().strftime("%Y-%M-%d %H:%M:%S")
        
        try:
            with open(self.file_path, mode, newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=data_header)
                
                # Write header only in write mode or if file is new/empty
                if mode == 'w' or (mode == 'a' and os.path.getsize(self.file_path) == 0 if os.path.exists(self.file_path) else True):
                    writer.writeheader()
                    # Add a comment with timestamp for traceability
                    # f.write(f"# Generated at {action_time}\n")
                
                # Write or append rows
                writer.writerows(data_set)
            
            action = "written" if mode == 'w' else "appended"
            print(f"CSV file '{self.file_path}' {action} successfully at {action_time}{' '+print_remarks if print_remarks is not None else ''}.")
            return True
            
        except PermissionError:
            print(f"Error: Permission denied when accessing '{self.file_path}'.")
            return False
        except OSError as e:
            print(f"Error: Could not access '{self.file_path}': {str(e)}")
            return False
        except Exception as e:
            print(f"Unexpected error while writing to '{self.file_path}': {str(e)}")
            return False