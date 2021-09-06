from typing import Any, Dict, List
import itertools
from SimpleDyDb.UpdateItemExpressions import UpdateItemExpressions
from SimpleDyDb.UpdateItemInstructions import UpdateItemsInstructions
import boto3
import math
from cache import GlobalCache
from helpers import random_string
import uuid
import re
from itertools import islice


def obj_chunks(obj:dict, chunk_size:int):
    """
    Splits an object into chunks of size chunk_size
    """
    it = iter(obj)
    for i in range(0, len(obj), chunk_size):
        yield {k:obj[k] for k in islice(it, chunk_size)}    

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def generate_update_item_instruction_chunks(update_item_instructions: UpdateItemsInstructions, batch_factor=1):
    # Get all attributes of update_item_instructions
    
    chunks_size_updates = int(math.ceil(len(update_item_instructions.updates) / batch_factor))
    chunks_size_deletes = int(math.ceil(len(update_item_instructions.deletes) / batch_factor))

    all_updates = list(obj_chunks(update_item_instructions.updates, chunks_size_updates)) if len(update_item_instructions.updates.keys()) > 0 else []
    all_deletes = list(chunks(update_item_instructions.deletes, chunks_size_deletes)) if len(update_item_instructions.deletes) > 0 else []
    
    max_chunks = max(len(all_updates), len(all_deletes))

    for i in range (0,max_chunks):
        _yield = UpdateItemsInstructions()
        try:    
            if all_updates[i]:
                _yield.updates = all_updates[i]
                has_update = True
        except:
            pass
            
        try:    
            if all_deletes[i]:
                _yield.deletes = all_deletes[i]
                has_delete = True
        except:
            pass

        yield _yield

    


def generate_attribute_name():
    # Strip all characters that are not alphanumeric, underscore, dash, or dot
    return "#"+str(uuid.uuid4()).replace("-", "")
    # return a


def generate_attribute_name_value():
    # Strip all characters that are not alphanumeric, underscore, dash, or dot
    return ":"+str(uuid.uuid4()).replace("-", "")
    # return b



def generate_expressions(update_item_instructions: UpdateItemsInstructions, batch_factor = 1) -> List[UpdateItemExpressions]:
    update_item_expressions_batches =  list(generate_update_item_instruction_chunks(update_item_instructions, batch_factor) if batch_factor > 1 else [update_item_instructions])

    for expr in update_item_expressions_batches:

        updates = expr.updates
        deletes = expr.deletes
        
        # Build expression_attribute_names, expression_attribute_values by assigning str(uuid.uuid4()) to each key
        # update_expressions should represent UpdateExpression for dynamodb attribute
        has_updates = None
        has_deletes = None

        expression_attribute_names = {}
        expression_attribute_values = {}
        update_expression_query = ""
        if len(updates.keys()):
            has_updates = True
            

            expression_attribute_values_inverted = {}

            update_expressions = []

            for real_name in updates:
                # try:
                this_expression_attribute_name = generate_attribute_name()

                this_expression_attribute_values = [

                ]

                expression_attribute_names[this_expression_attribute_name] = real_name
                string_update_value = ""

                if type(updates[real_name]) == list and len(updates[real_name]) > 0:
                    real_value = updates[real_name][0]
                    is_list_append = False
                    if type(real_value) == list:
                        is_list_append = True

                    if not is_list_append:
                        if real_value in expression_attribute_values_inverted:
                            # Avoid generating new UUID. Save bandwith by using existing UUID
                            expr_attr_value_key = expression_attribute_values_inverted[real_value]
                            expression_attribute_values[expr_attr_value_key] = real_value
                        else:
                            expr_attr_value_key = generate_attribute_name_value()
                            expression_attribute_values_inverted[real_value] = expr_attr_value_key
                            expression_attribute_values[expr_attr_value_key] = real_value
                        this_expression_attribute_values.append(real_value)
                        string_update_value = "%s = if_not_exists(%s, %s)" % (
                            this_expression_attribute_name, this_expression_attribute_name, expr_attr_value_key)
                    else:
                        expr_attr_value_key = generate_attribute_name_value()
                        expression_attribute_values[expr_attr_value_key] = real_value
                        this_expression_attribute_values.append(expr_attr_value_key)
                        empty_list_value = []
                        expr_attr_value_key_empty_list = generate_attribute_name_value()
                        expression_attribute_values[expr_attr_value_key_empty_list] = empty_list_value
                        # #exceptions = list_append(if_not_exists(#exceptions,:empty_list),:err)
                        string_update_value = "%s = list_append(if_not_exists(%s, %s), %s)" % (
                            this_expression_attribute_name, this_expression_attribute_name, expr_attr_value_key_empty_list, expr_attr_value_key)
                    if len(updates[real_name]) > 1:
                        real_value = updates[real_name][1]

                        if real_value in expression_attribute_values_inverted:
                            # Avoid generating new UUID. Save bandwith by using existing UUID
                            expr_attr_value_key = expression_attribute_values_inverted[real_value]
                        else:
                            expr_attr_value_key = generate_attribute_name_value()
                            expression_attribute_values[expr_attr_value_key] = real_value
                        this_expression_attribute_values.append(real_value)
                        string_update_value += " + " + \
                            expr_attr_value_key
                else:
                    real_value = updates[real_name]

                    if repr(updates[real_name]) in expression_attribute_values_inverted:
                        # Avoid generating new UUID. Save bandwith by using existing UUID
                        expr_attr_value_key = expression_attribute_values_inverted[real_value]
                    else:
                        expr_attr_value_key = generate_attribute_name_value()
                        expression_attribute_values[expr_attr_value_key] = real_value
                    this_expression_attribute_values.append(real_value)
                    string_update_value = "%s = %s" % (
                        this_expression_attribute_name, expr_attr_value_key)

                update_expressions.append(string_update_value)
        
            update_expression_query = "SET " + ", ".join(update_expressions)
        if len(deletes):
            has_deletes = True  
            for delete in deletes:
                expression_attribute_names[generate_attribute_name()] = delete
            
            update_expression_query += " REMOVE " + ", ".join(
                list(expression_attribute_names.keys())
            )
        
        if not has_updates and not has_deletes:
            raise "Nothing to update"

        update_expression_query = update_expression_query.strip()
        yield UpdateItemExpressions(update_expression_query, expression_attribute_names, expression_attribute_values, updates ,deletes)
        



def subtract_items_from_list(items_to_subtract, list_to_subtract_from):
    # Subtract items from list
    # Return a list of items that were removed
    #removed_items = []
    for item in items_to_subtract:
        if item in list_to_subtract_from:
            list_to_subtract_from.remove(item)
    return list_to_subtract_from

def subtract_keys_to_dict(keys_to_subtract: List[str], dict_to_subtract: Dict[str, Any]):
    for key in keys_to_subtract:
        if key in dict_to_subtract:
            del dict_to_subtract[key]
    return dict_to_subtract