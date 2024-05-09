#!/bin/sh

set -e

file_date=$1
file_hour=$2
file_date_n_hour="${file_date}-${file_hour}"
gz_filename="${file_date_n_hour}.json.gz"
download_dir="../data/raw_gh_data/${file_date}"
mkdir -p $download_dir
wget -P $download_dir "https://data.gharchive.org/${gz_filename}"

dest_dir="../data/watch_event_data/hourly/${file_date}"
mkdir -p $dest_dir
dest_filename="${dest_dir}/watch_${file_date_n_hour}.json"
gzip -cd < "${download_dir}/${gz_filename}" | grep -F "WatchEvent" >> $dest_filename

set +e 
