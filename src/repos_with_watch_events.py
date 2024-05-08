import json
from datetime import datetime as dt

from dotenv import dotenv_values, find_dotenv
import requests

config = dotenv_values(find_dotenv())

# Copied from devranker_core.utils.github_request Ned
# TODO: Remove the function and refer from a standard package
def get_github_api_token():
	return config['GITHUB_API_KEY']

# Copied from devranker_core.utils.github_request Ned
# TODO: Remove the function and refer from a standard package
def get_github_request_headers():
	return {"Authorization": f"bearer {get_github_api_token()}"}

# Copied from devranker_core.utils.github_request Ned
# TODO: Remove the function and refer from a standard package
def request_github_api(path, follow_links = False, extract_records=None):
	headers = get_github_request_headers()
	print(f'Log: Requesting GitHub API Endpoint: {path}')
	request = requests.get('https://api.github.com/'+path, headers=headers)
	if request.status_code == 200:
		if follow_links and 'next' in request.links:
			next_endpoint = request.links['next']['url'].split('.com/', 1)[1]
			results = extract_records(request.json()) if extract_records else request.json()
			results += request_github_api(next_endpoint, follow_links=True, extract_records=extract_records)
			return results
		return extract_records(request.json()) if extract_records else request.json()
	else:
		raise Exception("API Request {} failed, returned status_code {}. Message: {}".format(path, request.status_code, request.text))

def load_json_line_records(data_file):
	with open(data_file) as input_file:
		records = input_file.read().strip().split('\n')
		json_records = [json.loads(record) for record in records]
	return json_records

def filter_records_and_store(event_type ,data_file, dest_file):
	records = load_json_line_records(data_file)
	print(f"Records Found: {len(records)}")
	watch_records = [record for record in records if record['type'] == event_type]
	print(f"`{event_type}` Found: {len(watch_records)}")
	store_json_records(watch_records, dest_file)

def store_json_records(json_records, dest_file):
	with open(dest_file, 'w') as output_file:
		json.dump(json_records, output_file)

def load_json_records(data_file):
	with open(data_file) as input_file:
		json_records = json.load(input_file)
	return json_records

def init_repo_count_dict(repo):
	repo_count_dict = {
		'repo_id': repo['id'],
		'repo_full_name': repo['name'],
		'repo_link': f"https://github.com/{repo['name']}",
		'repo_github_api_url': repo['url'],
		'new_stars': 0,
		'rank': 0,
		'starring_actors': set(),
		'repository_details': dict()
	}
	return repo_count_dict

def get_repository_details(repo_id):
	github_repository_details = request_github_api(f'repositories/{repo_id}')
	repository_details = {
		'name': github_repository_details['name'],
		'owner_login': github_repository_details['owner']['login'],
		'owner_avatar_url': github_repository_details['owner']['avatar_url'],
		'owner_type': github_repository_details['owner']["type"],
		'description': github_repository_details['description'],
		'created_at': github_repository_details['created_at'],
		'updated_at': github_repository_details['updated_at'],
		'pushed_at': github_repository_details['pushed_at'],
		'repo_size': github_repository_details['size'],
		'stargazers_count': github_repository_details['stargazers_count'],
		'language': github_repository_details['language'],
		'forks_count': github_repository_details['forks_count'],
		'archived': github_repository_details['archived'],
		'open_issues_count': github_repository_details['open_issues_count'],
		'license': github_repository_details['license'],
		'is_template': github_repository_details['is_template'],
		'topics': github_repository_details['topics'],
		'default_branch': github_repository_details['default_branch']
	}
	return repository_details

def get_repository_star_counts(watch_data_file):
	watch_records = load_json_records(watch_data_file)
	max_time = dt.min
	starred_counts = {}
	actor_starred = {}
	for record in watch_records:
		repo_id = record['repo']['id']
		actor_id = record['actor']['id']

		# Update Repo Count
		repo_count_dict = starred_counts.get(repo_id, init_repo_count_dict(record['repo']))
		repo_count_dict['new_stars'] = repo_count_dict['new_stars'] + 1
		repo_count_dict['starring_actors'].add(actor_id)
		starred_counts[repo_id] = repo_count_dict

		# Update Actor Count
		actor_repos = actor_starred.get(actor_id, list())
		actor_repos.append(repo_id)
		actor_starred[actor_id] = actor_repos

		record_date = dt.strptime(record['created_at'], "%Y-%m-%dT%H:%M:%SZ")
		if record_date > max_time:
			max_time = record_date

	count = 0
	top_repos_list = []
	top_repo_ids = []
	for repo_id, repo_star_count in sorted(starred_counts.items(), key=lambda item: item[1]['new_stars'], reverse=True):
		repo_star_count['rank'] = count
		try:
			repo_star_count['repository_details'] = get_repository_details(repo_star_count['repo_id'])
		except Exception:
			continue
		top_repos_list.append(repo_star_count)
		top_repo_ids.append(repo_id)
		# print(f"https://github.com/{repo_star_count['repo_full_name']}, {repo_star_count['new_stars']}")
		count += 1
		if count == 30:
			break

	for top_repo in top_repos_list:
		also_starred_repos = []
		for author in top_repo['starring_actors']:
			also_starred_repos.extend(actor_starred[author])
		top_also_starred = sorted([[x,also_starred_repos.count(x)] for x in set(also_starred_repos) if x not in top_repo_ids], key=lambda item: item[1], reverse=True)[:6]
		top_repo['also_starred'] = [[star_counts,starred_counts[repo].copy()] for repo,star_counts in top_also_starred ]
		del top_repo['starring_actors']
		stale_repo_indices = []
		for index in range(len(top_repo['also_starred'])):
			del top_repo['also_starred'][index][1]['starring_actors']
			try:
				top_repo['also_starred'][index][1]['repository_details'] = get_repository_details(top_repo['also_starred'][index][1]['repo_id'])
			except Exception:
				stale_repo_indices.insert(0,index)
		for index in stale_repo_indices:
			top_repo['also_starred'].pop(index)

	trending_results = {
		"total_stars": len(watch_records),
		"latest_event_time": max_time.timestamp(),
		"repo_list": top_repos_list
	}
	return trending_results

def combine_json_records(data_files, dest_file, json_lines=False):
	combined_records = []
	for file in data_files:
		if json_lines:
			combined_records.extend(load_json_line_records(file))
		else:
			combined_records.extend(load_json_records(file))
	store_json_records(combined_records, dest_file)

if __name__ == '__main__':
	date = '2024-05-02'
	hour_start = 1
	hour_end = 2

	"""
		Filtering WatchEvents
	"""
	for hour in range(hour_start, hour_end):
		data_file = f'../data/raw_gh_data/{date}-{hour}.json'
		dest_file = f'../data/watch_event_data/hourly/{date}/watch_{date}-{hour}.json'
		event_type = 'WatchEvent'
		filter_records_and_store(event_type, data_file, dest_file)

	"""
		Combine hourly data to daily
	"""
	# hourly_files = [f'../data/watch_event_data/hourly/{date}/watch_{date}-{hour}.json' for hour in range(hour_start,hour_end)]
	# dest_file = f'../data/watch_event_data/daily/watch_{date}.json'
	# combine_json_records(hourly_files, dest_file)
	# watch_data_file = dest_file

	# import sys
	# watch_data_file = sys.argv[1] #'../data/watch_event_data/hourly/watch_2024-04-23-2.json'
	# trending_repos = get_repository_star_counts(watch_data_file)
	# from pprint import pprint
	# pprint(trending_repos)
	# store_json_records(trending_repos, f'../output/trending_repos_{date}.json')

