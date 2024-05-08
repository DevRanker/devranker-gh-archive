from download_gh_file import get_latest_gh_file_date, download_gh_file_from_date
from repos_with_watch_events import filter_records_and_store, combine_json_records, get_repository_star_counts, store_json_records
from datetime import timedelta
from pathlib import Path
import sys

if __name__ == '__main__':
	latest_gh_file_date = get_latest_gh_file_date()
	print(f"Latest File Date: {latest_gh_file_date}")	
	latest_hourly_files = []
	for i in range (24):
		file_hour = latest_gh_file_date - timedelta(hours=i)
		watch_event_file = f'../data/watch_event_data/hourly/{file_hour.date()}/watch_{file_hour.date()}-{file_hour.hour}.json'
		if not Path(watch_event_file).is_file():
			print(f"Downloading File for: {file_hour}")
			hourly_file_path = download_gh_file_from_date(file_hour)
			if not hourly_file_path:
				print(f"Error Downloading the file for: {file_hour}")
				sys.exit(1)
			watch_events_directory = watch_event_file.rsplit('/',1)[0]
			Path(watch_events_directory).mkdir(parents=True, exist_ok=True) 
			event_type = 'WatchEvent'
			print(f'Filtering {event_type} Records for : {file_hour}')
			filter_records_and_store(event_type, hourly_file_path, watch_event_file)
			Path.unlink(hourly_file_path)
		latest_hourly_files.append(watch_event_file)
	print(f'Combining lastest 24-Hours Records')
	latest_24_hour_watch_events_file = f'../data/watch_event_data/trending_repos/watch_{latest_gh_file_date.date()}-{latest_gh_file_date.hour}.json'
	combine_json_records(latest_hourly_files, latest_24_hour_watch_events_file)
	print(f'Generating Trending Repos Report')
	trending_repos = get_repository_star_counts(latest_24_hour_watch_events_file)
	store_json_records(trending_repos, f'../output/trending_repos_{latest_gh_file_date.date()}-{latest_gh_file_date.hour}.json')