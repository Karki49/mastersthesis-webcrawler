cd ../src ;
celery -A celeryapp worker -Q crawl_job_q --loglevel=INFO -c 2 --max-tasks-per-child=1;
