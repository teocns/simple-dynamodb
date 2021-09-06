

class UpdateItemsInstructions:
    """Class to hold instructions for updating DynamoDB records."""

    updates: dict
    deletes: dict



    def __init__(
        self,
        updates: dict = {},
        deletes: dict = []
    ) -> None:
        """Save updates and deletes."""
        self.updates = updates
        self.deletes = deletes
    
        
        