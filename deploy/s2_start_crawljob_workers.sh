cd ../src ;
celery -A celeryapp worker -Q crawl_job_q --loglevel=INFO -c 2 --prefetch-multiplier=1 -Ofair --max-tasks-per-child=1;

#celery -A celeryapp worker -Q crawl_job_q --loglevel=INFO -c 4 --prefetch-multiplier=1 -Ofair --max-tasks-per-child=1;
#celery -A celeryapp worker -Q etl_job_q --loglevel=INFO -c 8 --prefetch-multiplier=100 --max-tasks-per-child=1000;
#celery -A celeryapp worker -Q etl_job_q --loglevel=INFO -c 1 --prefetch-multiplier=1 --max-tasks-per-child=5;