uri='036497184801.dkr.ecr.us-east-1.amazonaws.com/docker-writting-elastic'
repository_name="docker-writting-elastic"
aws_account_id="036497184801"
region='us-east-1'
aws_role='arn:aws:iam::036497184801:role/vivanta-lambda-stg'
env='p'
environment='production'
create_lambda='N'

echo "Enter the environment (default: $env, development=d, production=p, staging=s)"
read env_input

echo "Create new lambda function? (Yy/Nn)"
read input
if [ $input == 'Y' ] || [ $input == 'y' ]; then
  create_lambda='Y'
fi

if [ "$env_input" == 'd' ]; then
    env='d'
    environment='dev'
elif [ "$env_input" == 's' ]; then
    env='s'
    environment='stg'
fi
env_file=".env.$environment"

#authenticate no matter what
aws ecr get-login-password --region $region | docker login --username AWS --password-stdin $aws_account_id.dkr.ecr.us-east-1.amazonaws.com

# if not uri
if [ -z "$uri" ]; then
    echo "Enter the repository URI (default: '')"
    read input
    uri=$input

    if [ -z "$uri" ]; then
        echo "Enter the repository name (default: $repository_name)"
        read input
        if [ ! -z "$input" ]; then
            repository_name=$input
        fi
        aws ecr create-repository --repository-name $repository_name --region $region --image-scanning-configuration scanOnPush=true --image-tag-mutability MUTABLE
        exit 0
    fi
fi

echo "Rebuilding image..."
bash build.sh
echo "Preparing environment variables..."
cp $env_file .env.aws
tr '\n' ',' < .env.aws > .env.tmp
mv .env.tmp .env.aws
env=$(cat .env.aws)

echo "Tagging image..."
docker tag $repository_name $aws_account_id.dkr.ecr.$region.amazonaws.com/$repository_name:latest

echo "Pushing image..."
docker push $aws_account_id.dkr.ecr.$region.amazonaws.com/$repository_name:latest

lambda_name=$repository_name'-'$environment
if [ $create_lambda == 'Y' ]; then
  echo "Creating lambda function..."
  aws lambda create-function \
    --architecture arm64 \
    --function-name $lambda_name \
    --package-type Image \
    --code ImageUri=$aws_account_id.dkr.ecr.$region.amazonaws.com/$repository_name:latest \
    --role $aws_role \
    --environment "Variables={$env}" \
    --vpc-config SubnetIds=subnet-0deef7c7db1b35a70,subnet-08c72e4ac202b6198,SecurityGroupIds=sg-01e5b7c150d395d4d \
    --timeout 60 \
    --memory-size 512

else
  echo "Updating lambda function..."
  aws lambda update-function-code \
    --function-name $lambda_name \
    --image-uri $aws_account_id.dkr.ecr.$region.amazonaws.com/$repository_name:latest
  sleep 20
  aws lambda update-function-configuration \
    --function-name $lambda_name \
    --environment "Variables={$env}" \
    --timeout 60 \
    --memory-size 512 \
    --vpc-config SubnetIds=subnet-0deef7c7db1b35a70,subnet-08c72e4ac202b6198,SecurityGroupIds=sg-01e5b7c150d395d4d
fi