# twotier-fibonacci
fibonacci broken out between web and worker tier

to deploy to a remote HP Helion Development Platform Trial cluster, installed following instructions at http://docs.hpcloud.com/helion/devplatform/1.1/ALS-developer-trial-quick-start/


1. run ./prep-deploy.sh
2. cd deploy
3. helion push -n

Or you can deploy individual services from web and worker directories respectively. The deploy directory is created to copy shared resources into deploy directories. 

required services:
* rabbit-mq
* mysql

To do local debugging, you need accessible instances of mysql and rabbit_mq. You should set the variables: 
* RABBITMQ_URL to amqp://{user}:{password}@{host}:{port}/%2f (note: default port is 5672, on localhost default user:password is guest:guest)
* MYSQL_URL to mysql://{user}:{password}@{host}:{port}/{database_name} (note: default port is 3306)

You need to create your database and populate it. You can do this for web and worker by running the ./create_populate_db.py scripts from deploy/web and deploy/worker. 

after creating the database, you will be ready to debug (rabbit queues are created by web and worker)

It is recommended that you  debug worker_wsgi.py from the ./deploy/worker directory and web_wsgi from the ./deploy/web directory. 

Note that the changes you make in web and worker top level directories need to be pushed out to ./deploy/web and ./deploy/worker by running ./prep_deploy.sh





