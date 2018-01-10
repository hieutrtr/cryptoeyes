#!/bin/bash
version="latest"
if [ -n "$1" ]; then
  version=$1
fi
name="cryptoeyes"
repo="hieutrtr"

echo "Creating Docker container"
result=`docker build --no-cache -t $name:$version . | tail -n1`
echo $result

if [[ ${result} != *"Successfully"* ]]; then
    echo "Build failed"
    echo $result
    exit
fi
id=`echo $result | cut -d ' ' -f3`

docker login --username "hieutrtr" --password "1547896sS"

echo "Tagging and pushing to Docker hub"
docker tag $id $repo/$name:$version
result=`docker push $repo/$name:$version`
if [[ ${result} != *"$version"* ]]; then
    echo "Push failed"
    echo $result
    exit
fi
echo $result
