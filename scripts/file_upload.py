import boto3
import os

def upload_file_to_s3(file_path, bucket_name, object_name=None):
    """
    Uploads a file to an S3 bucket.
    
    :param file_path: Path to the file to upload
    :param bucket_name: Name of the S3 bucket
    :param object_name: S3 object name. If not specified, uses the file name
    :return: True if file was uploaded, else False
    """
    # If S3 object_name was not specified, use the file_path basename
    if object_name is None:
        object_name = os.path.basename(file_path)

    # Create an S3 client
    s3_client = boto3.client('s3')
    
    try:
        s3_client.upload_file(file_path, bucket_name, object_name)
        print(f"File '{file_path}' successfully uploaded to '{bucket_name}' as '{object_name}'.")
        return True
    except FileNotFoundError:
        print(f"The file '{file_path}' was not found.")
        return False
    except Exception as e:
        print(f"An error occurred: {e}")
        return False

if __name__ == '__main__':
    # Define file and S3 bucket details
    csv_file = '/opt/airflow/data/transactions.csv'
    s3_bucket = 'retail-transactions'  # ⚠️ Replace with your actual S3 bucket name
    
    # Upload the file
    upload_file_to_s3(csv_file, s3_bucket)