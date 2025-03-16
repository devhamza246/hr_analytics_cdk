# HR Analytics API - AWS CDK Deployment

## Overview
This project deploys an **HR Analytics API** using **AWS CDK (Python)**. The infrastructure includes:
- **DynamoDB** (to store HR analytics data)
- **Lambda Functions** (to process API requests)
- **API Gateway** (to expose endpoints)
- **Cognito** (for authentication and authorization)

## Prerequisites
Before deploying this project, ensure you have the following installed:
- [AWS CLI](https://aws.amazon.com/cli/) (Configured with credentials)
- [Node.js](https://nodejs.org/) (Required for AWS CDK)
- [AWS CDK](https://docs.aws.amazon.com/cdk/v2/guide/home.html) (`npm install -g aws-cdk`)
- [Python 3.12.9](https://www.python.org/downloads/)

## Installation
1. **Clone the repository:**
   ```sh
   git clone https://github.com/your-repo/hr-analytics-cdk.git
   cd hr-analytics-cdk
   ```
2. **Create a virtual environment and install dependencies:**
   ```sh
   python -m venv .venv
   source .venv/bin/activate  # For Mac/Linux
   .venv\Scripts\activate     # For Windows
   pip install -r requirements.txt
   ```

3. **Bootstrap AWS CDK (Only needed for the first deployment):**
   ```sh
   cdk bootstrap
   ```

## Deploying the Infrastructure
Run the following command to deploy the stack:
```sh
cdk deploy
```
This will provision all AWS resources, including:
- DynamoDB Table (`HRAnalyticsTable`)
- API Gateway (`HRAnalyticsAPI`)
- Lambda Functions (`usage`, `categories`, `performance`, `demographics`)
- Cognito User Pool (for authentication)

## Adding Sample Data to DynamoDB
Once the infrastructure is deployed, you can manually insert sample data into DynamoDB:
```sh
python scripts/populate_data.py
```
This script inserts test records for API testing.

## API Endpoints
| API Name       | Method | Endpoint | Description |
|---------------|--------|----------|-------------|
| Usage         | GET    | `/usage` | Fetches user query statistics |
| Categories    | GET    | `/categories` | Retrieves category distribution |
| Performance   | GET    | `/performance` | Computes satisfaction and resolution rates |
| Demographics  | GET    | `/demographics` | Returns department and user demographic stats |

### Example API Request (via `curl`)
```sh
curl -X GET "https://your-api-id.execute-api.region.amazonaws.com/prod/usage"
```

## Cleaning Up Resources
To delete all provisioned AWS resources:
```sh
cdk destroy
```

## Troubleshooting
- If you get permission errors, ensure your AWS CLI credentials are configured properly (`aws configure`).
- If `cdk deploy` fails, check for missing dependencies and run `pip install -r requirements.txt`.
- Verify that you have the necessary IAM permissions to deploy AWS resources.

## License
This project is licensed under the MIT License.