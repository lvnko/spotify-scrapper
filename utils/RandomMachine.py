import random, csv
import pandas as pd
from datetime import time

class RandomMachine:

    def __init__(self, ph_email_domains=None, gender_dict=None):
        default_ph_email_domains = ["gmail.com", "instagram.com", "spotify.com"]
        default_gender_dict = { 'Female': 'women', 'Male': 'men' }
        self.__ph_email_domains = default_ph_email_domains + ph_email_domains if ph_email_domains is not None else default_ph_email_domains
        self.__gender_dict = gender_dict if gender_dict is not None else default_gender_dict

    def get_random_nums(self, offset=0, pool_size=100, len=1, sorted=True, no_repeat=False):
        
        if no_repeat is True and len > pool_size:
            raise ValueError("Length cannot be greater than pool_size to ensure uniqueness")
        
        random_numbers = [random.randint(0+offset, pool_size-1+offset) for _ in range(len)] if no_repeat is False else random.sample(range(offset, pool_size + offset), len)
        # print(random_numbers)
        if sorted is True:
            random_numbers.sort()
        return random_numbers
    
    def get_random_time(self):
        # Generate random hours, minutes, seconds
        hours = random.randint(0, 23)
        minutes = random.randint(0, 59)
        seconds = random.randint(0, 59)

        # Create a time object
        random_time = time(hours, minutes, seconds)

        # Format as string (e.g., "14:35:22")
        return random_time.strftime("%H:%M:%S")
    
    def get_email_domain_placeholders_size(self):
        return len(self.__ph_email_domains)
    
    def create_random_email_addr(self, name="Lorem Name"):
        [random_email_domain_index] = self.get_random_nums(pool_size=self.get_email_domain_placeholders_size(), sorted=False)
        random_email_domain = self.__ph_email_domains[random_email_domain_index]
        result = f"{name.lower().replace(' ', '_')}@{random_email_domain}"
        return result
    
    def extract_csv_rows_pandas(self, csv_file, indices):
        """Extract specific rows from a CSV file using pandas based on indices."""
        try:
            # Read only the specified rows using pandas
            df = pd.read_csv(csv_file, skiprows=lambda x: x not in indices and x != 0, nrows=len(indices))
            # print('df',df)
            # print('df.index',df.index)
            # Reindex to match the provided indices (may need adjustment based on CSV structure)
            # df.index = [i for i in indices if i < len(df)]
            return df
        except FileNotFoundError:
            raise FileNotFoundError(f"CSV file {csv_file} not found")
        except Exception as e:
            raise Exception(f"Error processing CSV: {str(e)}")

    def extract_csv_rows_csv(self, csv_file, indices):
        """Extract specific rows from a CSV file using csv module based on indices."""
        try:
            indices_set = set(indices)  # For O(1) lookup
            extracted_rows = []
            with open(csv_file, 'r', newline='') as file:
                reader = csv.reader(file)
                header = next(reader)  # Store header
                extracted_rows.append(header)  # Include header in output
                for i, row in enumerate(reader):
                    if i in indices_set:
                        extracted_rows.append(row)
            return extracted_rows
        except FileNotFoundError:
            raise FileNotFoundError(f"CSV file {csv_file} not found")
        except Exception as e:
            raise Exception(f"Error processing CSV: {str(e)}")