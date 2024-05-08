from download_gh_file import get_latest_gh_file_date, download_gh_file_from_date
from repos_with_watch_events import filter_records_and_store, combine_json_records, get_repository_star_counts, store_json_records
from datetime import timedelta
from pathlib import Path
import sys
import subprocess

"""
	Function to download a file, uncompress it, filter events and write the result to a json file.
	The function was later replaced by a shell script:
		- Reading the uncompressed file after writing it, was killing the process due to memory limits
		- just file.read() operation was taking 6-8x the file size in RAM
"""
def download_file_and_filter_events(file_hour, dest_file, event_type='WatchEvent'):
	print(f"Downloading File for: {file_hour}")
	hourly_file_path = download_gh_file_from_date(file_hour)
	if not hourly_file_path:
		print(f"Error Downloading the file for: {file_hour}")
		sys.exit(1)
	filtered_events_directory = dest_file.rsplit('/',1)[0]
	Path(filtered_events_directory).mkdir(parents=True, exist_ok=True)
	print(f'Filtering {event_type} Records for : {file_hour}')
	filter_records_and_store(event_type, hourly_file_path, dest_file)
	Path.unlink(hourly_file_path)

if __name__ == '__main__':
	latest_gh_file_date = get_latest_gh_file_date()
	print(f"Latest File Date: {latest_gh_file_date}")
	latest_hourly_files = []
	for i in range (24):
		gh_file = latest_gh_file_date - timedelta(hours=i)
		file_date = gh_file.date()
		file_hour = gh_file.hour
		watch_event_file = f'../data/watch_event_data/hourly/{file_date}/watch_{file_date}-{file_hour}.json'
		if not Path(watch_event_file).is_file():
			download_status = subprocess.call(['sh', './download_and_filter_watch_events.sh', f'{file_date}', f'{file_hour}'])
		latest_hourly_files.append(watch_event_file)
	print(f'Combining lastest 24-Hours Records')
	trailing_24_hours_directory = '../data/watch_event_data/trailing_24_hours'
	Path(trailing_24_hours_directory).mkdir(parents=True, exist_ok=True)
	latest_24_hour_watch_events_file = f'{trailing_24_hours_directory}/watch_{latest_gh_file_date.date()}-{latest_gh_file_date.hour}.json'
	combine_json_records(latest_hourly_files, latest_24_hour_watch_events_file, json_lines=True)
	print(f'Generating Trending Repos Report')
	trending_repos = get_repository_star_counts(latest_24_hour_watch_events_file)
	trending_repos_directory = f'../output/trending_repos/{latest_gh_file_date.date()}'
	Path(trending_repos_directory).mkdir(parents=True, exist_ok=True)
	store_json_records(trending_repos, f'{trending_repos_directory}/trending_repos_{latest_gh_file_date.date()}-{latest_gh_file_date.hour}.json')
	store_json_records(trending_repos, f'../output/trending_repos/trending_repos_latest.json')
