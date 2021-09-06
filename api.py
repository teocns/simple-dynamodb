import boto3
from SimpleDyDb.UpdateItemInstructions import UpdateItemsInstructions
from SimpleDyDb.helpers import generate_expressions, subtract_items_from_list, subtract_keys_to_dict





def update_item(
    table_resource,
    Key: str,
    update_instructions : UpdateItemsInstructions
):
    """
    Puts a new resource to the database.

    Args:
        resource (boto3.dynamodb.table): boto3 table resource
        Key: the key of the item to update

    Returns:
        nothing for now
    """

    current_batch_factorization = 1
    successfully_updated_keys  = []
    successfully_deleted_keys  = []
    all_success = False
    while not all_success:

        update_instructions.updates = subtract_keys_to_dict(successfully_updated_keys, update_instructions.updates)
        update_instructions.deletes = subtract_items_from_list(successfully_deleted_keys, update_instructions.deletes)

        update_instructions_batches = generate_expressions(
            update_instructions,
            current_batch_factorization
        )
        
        try:
            l = list(update_instructions_batches)
            it = 0
            for update_instructions_batch in l:
                print("Batch: {}".format(it))

                response = table_resource.update_item(
                    Key=Key,
                    ExpressionAttributeNames= update_instructions_batch.ExpressionAttributeNames,
                    ExpressionAttributeValues= update_instructions_batch.ExpressionAttributeValues,
                    UpdateExpression= update_instructions_batch.UpdateExpression
                )
                update_length  = len(update_instructions_batch.ExpressionAttributeNames)
                
                print('Updated successfully a batch of %s items' % update_length)
                
                # When some batch fails, we don't need to repeat updates on batches' attributes that were successfully executed
                successfully_updated_keys.extend(
                    list(update_instructions_batch.original_updates.keys())
                )
                successfully_deleted_keys.extend(
                    list(update_instructions_batch.original_deletes)
                )

            all_success = True  
        except Exception as ex:
            if "Expression size has exceeded the maximum allowed size" in str(ex):
                current_batch_factorization +=1
                print("Expression size exceeded the maximum allowed size. Retrying with batch factorization %s" % (current_batch_factorization))
            else:
                raise ex
        
