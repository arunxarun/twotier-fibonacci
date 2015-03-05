# twotier-fibonacci
fibonacci broken out between web and worker tier

to deploy to a remote HP Helion Development Platform Trial cluster, installed following instructions at http://docs.hpcloud.com/helion/devplatform/1.1/ALS-developer-trial-quick-start/


1. run ./prep-deploy.sh
2. cd deploy/web
3. helion push -n
4. cd ../worker
5. helion push -n

required services:
rabbit-mq
mysql


