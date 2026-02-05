# Arctic Shift User History Downloader

A multithreaded Python script to automate downloading Reddit user history (posts and comments) via the Arctic Shift API. Designed for bulk downloads from lists of users while handling rate limits, retries, and interruptions gracefully.

## Features
- Downloads posts and comments for a list of users.
- Resumes automatically if interrupted.
- Handles API rate limiting and exponential backoff.
- Multithreaded to speed up downloads.
- Verifies JSON file integrity before overwriting.
- Logs progress, errors, and completed users.

## Requirements
- Python 3.9+
- requests library (pip install requests)
- logging (standard library)
- queue and threading (standard library)

## Usage
1. Prepare your input file: A text file containing one username per line.
2. Edit configuration in the script such as file paths, number of concurrent users and end date.
3. Run the script with appropriate filepaths: python arcticshift_downloader.py
4. Logs are saved to api_download.log.
5. Files are saved in OUTPUT_DIR in JSON Lines format.
6. COMPLETED_FILE keeps track of finished users.

## Optional Improvements
- Add CLI arguments instead of hardcoding file paths.
- Support automatic splitting into smaller files for large users.
- Add progress bar with tqdm for each user’s download.
- Add field for start date if needed.
