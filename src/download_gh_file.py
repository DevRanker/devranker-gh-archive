from datetime import datetime, timedelta, timezone
import requests

def download_file_from_url(url, target):
	response = requests.get(url)
	if response.ok and response.status_code == 200:
		with open(target, 'wb') as output_file:
			output_file.write(response.content)
	return response.status_code

def get_latest_gh_file_date():
	return datetime.now(timezone.utc) - timedelta(hour=1)