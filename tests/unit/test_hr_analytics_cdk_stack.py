import aws_cdk as core
import aws_cdk.assertions as assertions

from hr_analytics_cdk.hr_analytics_cdk_stack import HrAnalyticsCdkStack

# example tests. To run these tests, uncomment this file along with the example
# resource in hr_analytics_cdk/hr_analytics_cdk_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = HrAnalyticsCdkStack(app, "hr-analytics-cdk")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
