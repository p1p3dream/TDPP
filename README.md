## Pre commit
Run the following commands to get pre-commit hooks installed:

```
pip install pre-commit
pre-commit install
```

## Manual deploy
You need to have awssam installed.

```
aws s3 sync glue s3://<your-s3-bucket>/glue
sam build
sam deploy --stack-name <your-stack-name> --region <your-region> --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM --guided
```
