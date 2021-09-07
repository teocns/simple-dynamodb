

class UpdateItemsInstructions:
    """Class to hold instructions for updating DynamoDB records."""

    updates: dict
    deletes: dict
    condition: str 
    key:dict

    def __init__(
        self,
        updates: dict = {},
        deletes: dict = [],
        key:dict = None,
        condition = None
    ) -> None:
        """
            updates={
                "my_increment_variable": [0,1] ,      #my_increment_variable = if_not_exists(#my_increment_variable,0) + :1
                "only_if_not_exists": ["alpha"] ,     #only_if_not_exists = if_not_exists(#only_if_not_exists,:alpha)
                "assigned_variable": "ciao" ,         #assigned_variable = :ciao
                "appendable": [[1]] ,                 #appendable = list_append(if_not_exists(#appendable,:[]), :[1])
            }

            deletes=[
                "attr1",
                "attr2",
            ]

            - Please note that the condition is optional.
            - Must pass attribute name starting with #
            - If the attribute name is a string, it must be enclosed in double quotes.
            - If the attribute name is a int, it must NOT be enclosed
            
            Example: 
            condition = "#a = :'asdf' AND #b >= :1234"
        """
        self.updates = updates
        self.deletes = deletes
        self.condition = condition
        self.key = key
        
        