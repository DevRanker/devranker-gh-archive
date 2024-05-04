from datetime import datetime, timedelta, timezone
import requests
import gzip
import shutil
import os

def download_file_from_url(url, target):
	response = requests.get(url)
	if response.ok and response.status_code == 200:
		with open(target, 'wb') as output_file:
			output_file.write(response.content)
	return response.status_code

def get_latest_gh_file_date():
	return datetime.now(timezone.utc) - timedelta(hours=1)

def uncompress_gz_file(gz_file, target_file, remove_original=True):
	with gzip.open(gz_file, 'rb') as f_in:
		with open(target_file, 'wb') as f_out:
			shutil.copyfileobj(f_in, f_out)
	if remove_original:
		os.remove(gz_file)

if __name__ == '__main__':
	latest_gh_file_date = get_latest_gh_file_date()
	latest_gh_file_name = f'{latest_gh_file_date.date()}-{latest_gh_file_date.hour}.json.gz'
	latest_gh_file_url = f'https://data.gharchive.org/{latest_gh_file_name}'
	new_file_download_path = f'../data/raw_gh_data/{latest_gh_file_name}'
	data_file_path = new_file_download_path.rsplit('.',1)[0]

	download_file_from_url(latest_gh_file_url, new_file_download_path)
	uncompress_gz_file(new_file_download_path, data_file_path)