from datetime import datetime, timedelta, timezone
import requests
import gzip
import shutil
from pathlib import Path

def download_file_from_url(url, target):
	response = requests.get(url)
	Path(target.rsplit('/',1)[0]).mkdir(parents=True, exist_ok=True) 
	if response.ok and response.status_code == 200:
		with open(target, 'wb') as output_file:
			output_file.write(response.content)
		return target
	else:
		print(response.content)

def download_gh_file_from_date(date, extract=True):
	gh_file_name = f'{date.date()}-{date.hour}.json.gz'
	gh_file_url = f'https://data.gharchive.org/{gh_file_name}'
	new_file_download_path = f'../data/raw_gh_data/{gh_file_name}'
	downloaded_file = download_file_from_url(gh_file_url, new_file_download_path)
	if extract and downloaded_file:		
		data_file_path = new_file_download_path.rsplit('.',1)[0]
		uncompress_gz_file(new_file_download_path, data_file_path)
		return data_file_path
	return downloaded_file

def get_latest_gh_file_date():
	return datetime.now(timezone.utc) - timedelta(hours=1)

def uncompress_gz_file(gz_file, target_file, remove_original=True):
	with gzip.open(gz_file, 'rb') as f_in:
		with open(target_file, 'wb') as f_out:
			shutil.copyfileobj(f_in, f_out)
	if remove_original:
		Path.unlink(gz_file)

if __name__ == '__main__':
	latest_gh_file_date = get_latest_gh_file_date()
	new_file_path = download_gh_file_from_date(latest_gh_file_date)
