#! /bin/bash


rm -rf deploy
mkdir deploy

for dest in web worker
do
	cp -r $dest deploy/$dest
	rm -rf deploy/$dest/test
	cp messaging/*.py deploy/$dest
	cp utils/*.py deploy/$dest
	cp ./manifest.yml deploy
done
