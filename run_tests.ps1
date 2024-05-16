$IMAGE_NAME="quantkit-test"
$CONTAINER_NAME="quantkit-test"

# build the Docker image if it does not already exist
if (!(docker images -q $IMAGE_NAME 2> $null)) { 
  # build the image
  Write-Output "Building Docker image $IMAGE_NAME"
  docker build --no-cache -f .\Dockerfile_dev -t $IMAGE_NAME .
}
else {
  Write-Output "Docker image $IMAGE_NAME already exists."
}

# build the container if it does not already exist
$c = docker ps -a -f name=$CONTAINER_NAME

if (!(([string]$c).split() -contains $CONTAINER_NAME)) { 
  Write-Output "Building Docker container $CONTAINER_NAME"
  $p = Get-Location

  docker run -d `
    -it `
    --name $CONTAINER_NAME `
    --mount type=bind,source="$p",target=/quantkit `
    $IMAGE_NAME /bin/bash
}
else {
  Write-Output "Docker container $CONTAINER_NAME already exists."
}

$TEST_LINE=@'
pytest --cov-config=tests/.coveragerc --cov=. 
tests/test_return_calc/test_simple_return.py 
-s
'@

docker start $CONTAINER_NAME
docker exec -it $CONTAINER_NAME /bin/bash -c ("${TEST_LINE}").replace("`n","").replace("`r","")
docker stop $CONTAINER_NAME