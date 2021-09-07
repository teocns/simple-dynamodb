from typing import Any, Dict, List
import itertools
from SimpleDyDb.ConditionExpression import ConditionExpression
from SimpleDyDb.UpdateItemExpressions import UpdateItemExpressions
from SimpleDyDb.UpdateItemInstructions import UpdateItemsInstructions
import boto3
import math
import uuid
import re
from itertools import islice
from SimpleDyDb.UpdateOperations import UpdateOperations

from SimpleDyDb.helpers import generate_attribute_name, generate_attribute_value_key, generate_update_item_instruction_chunks





class ExpressionBuilder:

    update_item_instructions: UpdateItemExpressions
    
    ExpressionAttributeNames: dict
    ExpressionAttributeValues: dict
    
    
    UpdateExpression = ""
    ConditionExpression = None


    # Single row strings, to later be joined into UpdateExpression
    __DeleteOperations = [] ##   ["#a = :c"]
    __UpdateOperations = [] ##   ["#a", "#c"]

    __ExpressionAttributeNames_inverse: dict
    __ExpressionAttributeValues_inverse: dict





    def __init__(self, update_item_expressions: UpdateItemsInstructions):
        self.__DeleteOperations = []
        self.__UpdateOperations = []
        self.ExpressionAttributeNames = {}
        self.ExpressionAttributeValues = {}
        self.__ExpressionAttributeNames_inverse = {}
        self.__ExpressionAttributeValues_inverse = {}
        self.ConditionExpression = None
        self.update_item_instructions = update_item_expressions

        
    
    def put_update_expression(self,operation_type: UpdateOperations, attribute_name: str, value: any):
        operation = None
        # Generate ExpressionAttributeName and ExpressionAttributeName keys
        attribute_name_key = self.put_expression_attribute_name(attribute_name)
        
        if operation_type == UpdateOperations.LIST_APPEND:
            # Retrieve empty list value
            default_value_key = self.put_expression_attribute_value([])
            update_value_key = self.put_expression_attribute_value(value[0])
            operation = "%s = list_append(if_not_exists(%s, %s), %s)" % (
                                    attribute_name_key, attribute_name_key, default_value_key, update_value_key)

        elif operation_type == UpdateOperations.INCREMENT:
            default_value_key = self.put_expression_attribute_value(value[0])
            update_value_key = self.put_expression_attribute_value(value[1])

            operation = "%s = if_not_exists(%s, %s) + %s" % (
                attribute_name_key, attribute_name_key, default_value_key, update_value_key
            )
        elif operation_type == UpdateOperations.SET_IF_NOT_EXISTS:
            update_value_key = self.put_expression_attribute_value(value[0])
            operation = "%s = if_not_exists(%s, %s)" % (
                attribute_name_key, attribute_name_key, update_value_key
            )

        elif operation_type == UpdateOperations.SET:
            update_value_key = self.put_expression_attribute_value(value)
            operation = "%s = %s" % (
                attribute_name_key, update_value_key
            )
        
        self.__UpdateOperations.append(operation)

    def put_delete_expression(self,attribute_name):
        attribute_name_key = self.put_expression_attribute_name(attribute_name)
        self.__DeleteOperations.append(attribute_name_key)

    def put_expression_attribute_name(self, attribute_name):
        """
        Returns: Generated or resued GUID attribute name
        """
        if attribute_name not in self.__ExpressionAttributeNames_inverse:
            generated_name = generate_attribute_name()
            self.__ExpressionAttributeNames_inverse[attribute_name] = generated_name
            self.ExpressionAttributeNames[generated_name] = attribute_name
            return generated_name
        else:
            return self.__ExpressionAttributeNames_inverse[attribute_name]


    def put_expression_attribute_value(self, value):
        """
        Returns: Generated or resued GUID attribute value key
        """
        # To save bandwith, we will only generate the attribute name value if the value does not exist in __ExpressionAttributeValues_inverse
        # Make sure the value is hashable and indexable
        hashable_value = repr(value)

        if hashable_value not in self.__ExpressionAttributeValues_inverse:
            attribute_value_key = generate_attribute_value_key()
            # If we get Unhashable type, then put the repr
            
            self.__ExpressionAttributeValues_inverse[hashable_value] = attribute_value_key
            self.ExpressionAttributeValues[attribute_value_key] = value
            return attribute_value_key
        else:
            return self.__ExpressionAttributeValues_inverse[hashable_value]
    
    
    def put_condition_expression(self, condition_expression: str):
        cond_obj = ConditionExpression( 
            condition_expression,
        )

        attribute_names_mappings = {}
        attribute_values_mappings = {}
        for attribute_name in cond_obj.ParsedAttributeNames:
            attribute_name_key = self.put_expression_attribute_name(str(attribute_name))
            attribute_names_mappings[attribute_name] = attribute_name_key
            

        for attribute_value in cond_obj.ParsedAttributeValues:
            attribute_value_key = self.put_expression_attribute_value(attribute_value)
            attribute_values_mappings[attribute_value] = attribute_value_key
        
        cond_obj.replace_with_expression_attribute_keys(
            attribute_names_mappings,
            attribute_values_mappings
        )
        
        self.ConditionExpression = cond_obj.Value


    def generate_update_expression(self):
        """
        Returns: Generated UpdateExpression
        """
        if len(self.__UpdateOperations):
            self.UpdateExpression = 'SET ' + ",  ".join(self.__UpdateOperations) 

        if len(self.__DeleteOperations):
            self.UpdateExpression += " REMOVE " + ", ".join(self.__DeleteOperations)


        return self.UpdateExpression.strip()

    

        

    @staticmethod
    def Generate(update_item_instructions, batch_factor = 1) -> List[UpdateItemExpressions]:
        """
            Yields generator of N amount of UpdateItemExpressions based on batch_factor
        """
        ExpressionBatches =  list(generate_update_item_instruction_chunks(update_item_instructions, batch_factor) if batch_factor > 1 else [update_item_instructions])

        for instructions_batch in ExpressionBatches:
            
            expression_builder = ExpressionBuilder(instructions_batch)

            updates = instructions_batch.updates
            deletes = instructions_batch.deletes
            
            has_updates = len(updates.keys())
            has_deletes = len(deletes)
            has_condition = bool(instructions_batch.condition)

            if has_updates:
                has_updates = True
                for attribute_name in updates:
                    value = updates[attribute_name]
                    update_operation = UpdateOperations.interpret(value)
                    expression_builder.put_update_expression(update_operation, attribute_name, value)
                
                

            if has_deletes:
                has_deletes = True
                for attribute_name in deletes:
                    expression_builder.put_delete_expression(attribute_name)

            

            
            if has_condition:
                expression_builder.put_condition_expression(
                    instructions_batch.condition
                    
                )

            if not has_updates and not has_deletes:
                raise "Nothing to update"

            UpdateExpression = expression_builder.generate_update_expression()    

            yield UpdateItemExpressions(UpdateExpression, expression_builder.ExpressionAttributeNames, expression_builder.ExpressionAttributeValues, expression_builder.ConditionExpression, updates ,deletes)
            

