from aws_cdk import (
    Stack,
    CfnOutput,
    RemovalPolicy,
    aws_cognito as cognito,
    aws_apigateway as apigateway,
    aws_lambda as _lambda,
    aws_dynamodb as dynamodb,
    aws_iam as iam,
)
from constructs import Construct


class HrAnalyticsCdkStack(Stack):

    def __init__(self, scope: Construct, id: str, **kwargs):

        super().__init__(scope, id, **kwargs)

        # Cognito User Pool
        user_pool = cognito.UserPool(
            self,
            "HRUserPool",
            self_sign_up_enabled=True,
            sign_in_aliases={"email": True},
            standard_attributes={"email": {"required": True, "mutable": False}},
            auto_verify={"email": True},
            removal_policy=RemovalPolicy.DESTROY,
        )

        # User Pool Domain
        user_pool_domain = user_pool.add_domain(
            "HRUserPoolDomain",
            cognito_domain=cognito.CognitoDomainOptions(
                domain_prefix="hr-analytics-demo"
            ),
        )

        # User Pool Client with OAuth
        user_pool_client = cognito.UserPoolClient(
            self,
            "HRUserPoolClient",
            user_pool=user_pool,
            generate_secret=False,  # Must be False for password auth
            auth_flows=cognito.AuthFlow(
                user_password=True,  # Allow password-based authentication
                user_srp=True,  # Secure authentication
            ),
            o_auth=cognito.OAuthSettings(
                flows=cognito.OAuthFlows(authorization_code_grant=True),
                callback_urls=[
                    "https://localhost:3000",
                ],
            ),
        )

        # DynamoDB Table
        analytics_table = dynamodb.Table(
            self,
            "HRAnalyticsTable",
            table_name="QueryAnalytics",
            partition_key=dynamodb.Attribute(
                name="user_id", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="timestamp", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,  # On-demand pricing
            removal_policy=RemovalPolicy.RETAIN,  # Avoid accidental deletion
        )

        # GSI for Category-based queries
        analytics_table.add_global_secondary_index(
            index_name="CategoryIndex",
            partition_key=dynamodb.Attribute(
                name="category", type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )

        # GSI for tracking query resolution (BOOLEAN stored as STRING)
        analytics_table.add_global_secondary_index(
            index_name="ResolvedIndex",
            partition_key=dynamodb.Attribute(
                name="resolved", type=dynamodb.AttributeType.STRING  # "true" | "false"
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )

        # GSI for Department usage tracking
        analytics_table.add_global_secondary_index(
            index_name="DepartmentIndex",
            partition_key=dynamodb.Attribute(
                name="department", type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )

        # GSI for Seniority-based analytics
        analytics_table.add_global_secondary_index(
            index_name="SeniorityIndex",
            partition_key=dynamodb.Attribute(
                name="seniority", type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )

        # GSI for satisfaction rating analysis
        analytics_table.add_global_secondary_index(
            index_name="SatisfactionIndex",
            partition_key=dynamodb.Attribute(
                name="satisfaction", type=dynamodb.AttributeType.NUMBER
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )

        # GSI for tracking query timestamps
        analytics_table.add_global_secondary_index(
            index_name="QueryTimestampIndex",
            partition_key=dynamodb.Attribute(
                name="query_timestamp", type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )

        # GSI for tracking response timestamps
        analytics_table.add_global_secondary_index(
            index_name="ResponseTimestampIndex",
            partition_key=dynamodb.Attribute(
                name="response_timestamp", type=dynamodb.AttributeType.STRING
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )

        # GSI for new users tracking
        analytics_table.add_global_secondary_index(
            index_name="NewUserIndex",
            partition_key=dynamodb.Attribute(
                name="new_user",
                type=dynamodb.AttributeType.STRING,  # Store as "true" | "false"
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )

        # Lambda Execution Role
        lambda_role = iam.Role(
            self,
            "HRLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonDynamoDBFullAccess"
                ),
            ],
        )

        # Lambda Functions
        usage_lambda = self.create_lambda(
            "UsageLambda", "usage_lambda.handler", lambda_role, analytics_table
        )
        categories_lambda = self.create_lambda(
            "CategoriesLambda",
            "categories_lambda.handler",
            lambda_role,
            analytics_table,
        )
        performance_lambda = self.create_lambda(
            "PerformanceLambda",
            "performance_lambda.handler",
            lambda_role,
            analytics_table,
        )
        demographics_lambda = self.create_lambda(
            "DemographicsLambda",
            "demographics_lambda.handler",
            lambda_role,
            analytics_table,
        )

        dashboard_stats_lambda = self.create_lambda(
            "DashboardStatsLambda",
            "dashboard_stats_lambda.handler",
            lambda_role,
            analytics_table,
        )

        # API Gateway
        api = apigateway.RestApi(
            self,
            "HRAnalyticsAPI",
            rest_api_name="HR Analytics Service",
            description="API for HR Assistant analytics dashboard",
        )

        # Cognito Authorizer
        authorizer = apigateway.CognitoUserPoolsAuthorizer(
            self, "APIAuthorizer", cognito_user_pools=[user_pool]
        )

        # API Endpoints
        metrics_resource = api.root.add_resource("metrics")

        self.create_api_method(metrics_resource, "usage", usage_lambda, authorizer)
        self.create_api_method(
            metrics_resource, "categories", categories_lambda, authorizer
        )
        self.create_api_method(
            metrics_resource, "performance", performance_lambda, authorizer
        )
        self.create_api_method(
            metrics_resource, "demographics", demographics_lambda, authorizer
        )
        self.create_api_method(
            metrics_resource, "dashboard_stats", dashboard_stats_lambda, authorizer
        )

        # Output User Pool ID & API Endpoint
        CfnOutput(self, "UserPoolId", value=user_pool.user_pool_id)
        CfnOutput(self, "UserPoolClientId", value=user_pool_client.user_pool_client_id)
        CfnOutput(self, "APIEndpoint", value=api.url)
        CfnOutput(self, "UserPoolDomain", value=user_pool_domain.domain_name)

    def create_lambda(self, id, handler, role, table):
        """Helper function to create Lambda functions."""
        return _lambda.Function(
            self,
            id,
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler=handler,
            code=_lambda.Code.from_asset("lambda"),
            role=role,
            environment={"DYNAMODB_TABLE": table.table_name},
        )

    def create_api_method(self, resource, name, lambda_function, authorizer):
        """Helper function to create API Gateway methods with CORS."""
        endpoint = resource.add_resource(name)

        # Allow CORS by adding OPTIONS method
        endpoint.add_method(
            "OPTIONS",
            apigateway.MockIntegration(
                integration_responses=[
                    apigateway.IntegrationResponse(
                        status_code="200",
                        response_parameters={
                            "method.response.header.Access-Control-Allow-Origin": "'*'",
                            "method.response.header.Access-Control-Allow-Methods": "'OPTIONS,GET'",
                            "method.response.header.Access-Control-Allow-Headers": "'Content-Type,Authorization'",
                        },
                    )
                ],
                passthrough_behavior=apigateway.PassthroughBehavior.WHEN_NO_MATCH,
                request_templates={"application/json": '{"statusCode": 200}'},
            ),
            method_responses=[
                apigateway.MethodResponse(
                    status_code="200",
                    response_parameters={
                        "method.response.header.Access-Control-Allow-Origin": True,
                        "method.response.header.Access-Control-Allow-Methods": True,
                        "method.response.header.Access-Control-Allow-Headers": True,
                    },
                )
            ],
        )

        # Add actual GET method
        endpoint.add_method(
            "GET",
            apigateway.LambdaIntegration(
                lambda_function,
                integration_responses=[
                    apigateway.IntegrationResponse(
                        status_code="200",
                        response_parameters={
                            "method.response.header.Access-Control-Allow-Origin": "'*'"
                        },
                    )
                ],
            ),
            authorization_type=apigateway.AuthorizationType.COGNITO,
            authorizer=authorizer,
            method_responses=[
                apigateway.MethodResponse(
                    status_code="200",
                    response_parameters={
                        "method.response.header.Access-Control-Allow-Origin": True
                    },
                )
            ],
        )
