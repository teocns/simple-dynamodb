from typing import Any, Dict, List
import itertools
from SimpleDyDb.UpdateItemExpressions import UpdateItemExpressions
from SimpleDyDb.UpdateItemInstructions import UpdateItemsInstructions
import boto3
import math
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


def generate_attribute_value_key():
    # Strip all characters that are not alphanumeric, underscore, dash, or dot
    return ":"+str(uuid.uuid4()).replace("-", "")
    # return b






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