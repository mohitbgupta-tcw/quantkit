param([string]$image, [string]$container)

# stop and remove the container if it exists
$c = docker ps -a -f name=$container

if (([string]$c).split() -contains $container) { 
  docker stop $image
  docker rm $container
}

# remove the Docker image if already exists
if (docker images -q $image 2> $null) {
  docker rmi $image
}