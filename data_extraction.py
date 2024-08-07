import random
import string
import pandas as pd
from faker import Faker
from google.cloud import storage
from pprint import pprint
import re

# Function to generate a list of dummy employee data
def generate_dummy_employee(num_employees):
    fake = Faker()
    employees = []
    departments = ['Engineering', 'Marketing', 'Sales', 'Human Resources', 'Finance', 'Customer Support']
    address_pattern = r'[,.|\n]'
    common_pattern = r'[^a-zA-Z0-9]'
    for _ in range(num_employees):
        employee = {
            'first_name': re.sub(common_pattern, "", fake.first_name()),
            'last_name': re.sub(common_pattern, "", fake.last_name()),
            'job_title': re.sub(common_pattern, "", fake.job()),
            'department': random.choice(departments),
            'email': fake.email(),
            
            'address': re.sub(address_pattern, ' ', fake.address()),
            'phone_number': fake.ssn(),
            'salary': round(random.uniform(30000, 150000), 2),
            'password': ''.join(random.choices(string.ascii_letters + string.digits, k=10))
            
        }
        # pprint(employee)
        employees.append(employee)

    return employees

# Function to validate data completeness
def validate_data(data):
    required_fields = ['first_name', 'last_name', 'job_title', 'department', 'email', 'address', 'phone_number', 'salary', 'password']

    for record in data:
        for field in required_fields:
            if not record.get(field):
                print(f"Validation failed: Missing value for '{field}' in record {record}")
                return False
    return True

# Function to write data to a CSV file
def write_to_csv(data, filename):
    df = pd.DataFrame(data)
    print(df)
    print(df.columns)

    df.to_csv(filename, index=False)

# Function to upload a file to a GCS bucket
def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(f"File {source_file_name} uploaded to {destination_blob_name}.")

# Main block to execute the script
if __name__ == "__main__":
    num_employees = 10000
    dummy_data = generate_dummy_employee(num_employees)

    csv_filename = 'dummy_employee_data.csv'
    write_to_csv(dummy_data, csv_filename)

    # Define GCS bucket details
    bucket_name = 'employee-data-dev'
    destination_blob_name = csv_filename

    # Upload the CSV file to GCS
    upload_to_gcs(bucket_name, csv_filename, destination_blob_name)
