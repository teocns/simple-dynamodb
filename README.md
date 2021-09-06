# Example to run
```

from SimpleDyDb.UpdateItemInstructions import UpdateItemsInstructions
from SimpleDyDb.helpers import subtract_keys_to_dict
from SimpleDyDb.api import update_item
import boto3



updates={
	"my_increment_variable": [0,1] ,# "my_increment_variable = if_not_exists(my_increment_variable,0) + 1"	,
	"only_if_not_exists": ["alpha"] ,# "only_if_not_exists = if_not_exists(only_if_not_exists,alpha)",
	"assigned_variable": "ciao" ,# "assigned_variable = ciao",
	"appendable": [[1]] ,# appendable = list_append(if_not_exists(appendable,[]), [1])
}

deletes=[
	"attr1",
	"attr2",
]

update_item(
    boto3.resource('dynamodb').Table('yourtable'),
    Key={
        "yyyy-mm-dd": "asd",
        "user_id": "asd"
    },

    update_instructions= UpdateItemsInstructions(updates,deletes),
)
```
