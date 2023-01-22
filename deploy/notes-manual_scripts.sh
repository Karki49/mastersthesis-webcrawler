# setup rabbitMQ
docker run --rm -it --name=rmq_thesis -p 5672:5672 -p 15672:15672 rabbitmq:3.9-management-alpine


# scylladb
nodetool snapshot --tag "iter<num>" --table alldomains crawl_state_db

#mongodb
mongodump -u root -p "pass..." --authenticationDatabase=admin -d crawl_state_db
mongorestore --drop --authenticationDatabase=admin -u root -p "..." --db "crawl_state_db" --dir "dump/crawl_state_db"

# redis
# SAVE (in console)

# log parsing
cat <logfile> | grep -onP "(.*spider.*started)|(.*spider.*finished)|(.*_count.*)"