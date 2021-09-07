allowed_limits_margin = 1.1
import math

class UpdateItemExpressions:
    UpdateExpression: str
    ExpressionAttributeNames: dict
    ExpressionAttributeValues: dict
    ConditionExpression: str


    original_updates: dict
    original_deletes : list
    original_condition_expression: str

    

    def __init__(
        self,
        UpdateExpression: str,
        ExpressionAttributeNames: dict,
        ExpressionAttributeValues: dict,
        ConditionExpression: str,
        original_updates: dict,
        original_deletes: list
    ):
        self.UpdateExpression = UpdateExpression
        self.ExpressionAttributeNames = ExpressionAttributeNames
        self.ExpressionAttributeValues = ExpressionAttributeValues
        self.original_updates = original_updates
        self.original_deletes = original_deletes
        self.ConditionExpression = ConditionExpression

