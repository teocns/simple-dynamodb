allowed_limits_margin = 1.1
import math

class UpdateItemExpressions:
    UpdateExpression: str
    ExpressionAttributeNames: dict
    ExpressionAttributeValues: dict
    


    original_updates: dict
    original_deletes : list

    allowed_limits = {
        "ExpressionAttributes": 2097152 * allowed_limits_margin,
        "UpdateExpression": 4096 * allowed_limits_margin,
        "Operators": 300 * allowed_limits_margin
    }

    def __init__(
        self,
        UpdateExpression: str,
        ExpressionAttributeNames: dict,
        ExpressionAttributeValues: dict,
        original_updates: dict,
        original_deletes: list
    ):
        self.UpdateExpression = UpdateExpression
        self.ExpressionAttributeNames = ExpressionAttributeNames
        self.ExpressionAttributeValues = ExpressionAttributeValues
        self.original_updates = original_updates
        self.original_deletes = original_deletes

