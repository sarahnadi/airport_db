import requests
from urllib.parse import urljoin
import os
import hashlib


def download_csv(url, local_filename, last_modified_header=None):
    """Downloads a CSV file from the given URL, optionally checking for updates.

    Args:
        url (str): The URL of the CSV file.
        local_filename (str): The local filename to save the downloaded file.
        last_modified_header (str, optional): The value of the Last-Modified header
            from a previous download. Defaults to None.

    Returns:
        bool: True if the file was downloaded or already exists and is up-to-date,
              False otherwise.
    """

    if os.path.exists(local_filename):
        if last_modified_header is None:
            # No previous download information, assume update required
            return download_file(url, local_filename)

        # Check if local file is up-to-date based on Last-Modified header
        with open(local_filename, 'rb') as f:
            local_hash = hashlib.md5(f.read()).hexdigest()

        response = requests.head(url)
        remote_hash = response.headers.get('Last-Modified', None)

        if local_hash == remote_hash:
            # Local file is up-to-date
            print(f"{local_filename} is already up-to-date.")
            return True
        else:
            # Local file needs update
            print(f"{local_filename} has been updated on remote server. Downloading...")
            return download_file(url, local_filename)
    else:
        # Local file doesn't exist, download it
        return download_file(url, local_filename)


def download_file(url, local_filename):
    """Downloads a file from the given URL."""

    response = requests.get(url, stream=True)
    response.raise_for_status()

    with open(local_filename, 'wb') as f:
        for chunk in response.iter_content(1024):
            f.write(chunk)

    print(f"{local_filename} downloaded successfully.")
    return True

def csv_download_initiallization():
    """Downloads CSV files from the GitHub link and checks for updates."""

    base_url = "https://raw.githubusercontent.com/davidmegginson/ourairports-data/main/"  # Replace with the actual GitHub link
    local_dir = "csv_files"  # You can change this directory

    os.makedirs(local_dir, exist_ok=True)  # Create the directory if it doesn't exist

    csv_filenames = ['airports.csv', 'countries.csv', 'navaids.csv', 'regions.csv',
                     'runways.csv', 'airport-frequencies.csv', 'airport-comments.csv']

    last_modified_headers = {}  # Dictionary to store last modified headers

    for filename in csv_filenames:
        url = urljoin(base_url, filename)
        local_filename = os.path.join(local_dir, filename)

        # Check for updates or download if file doesn't exist
        downloaded = download_csv(url, local_filename, last_modified_headers.get(filename))

        if downloaded:
            # Update last modified header for future checks
            response = requests.head(url)
            last_modified_headers[filename] = response.headers.get('Last-Modified', None)


if __name__ == "__main__":
    csv_download_initiallization()
