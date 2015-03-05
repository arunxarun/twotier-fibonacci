#! /bin/bash


rm -rf deploy
mkdir deploy

for dest in web worker
do
	cp -r $dest deploy/$dest
	cp messaging/*.py deploy/$dest
	cp data/*.py deploy/$dest
	cp utils/*.py deploy/$dest
done
