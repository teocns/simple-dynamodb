from typing import Set


class UpdateOperations:
    SET = 0
    SET_IF_NOT_EXISTS = 1
    INCREMENT = 2
    LIST_APPEND = 3

    @staticmethod
    def interpret(update_instruction_value):
        # Use only for "updates"
        # Do not use for deletes
        
        if type(update_instruction_value) is list:
            # Possibilities:
            # [ any ]
            # [ int, int ]
            # [ [ any ] ]

            if len(update_instruction_value) == 1:
                if type(update_instruction_value[0]) is not list:
                    return UpdateOperations.SET_IF_NOT_EXISTS
                else:
                    return UpdateOperations.LIST_APPEND
            else:
                return UpdateOperations.INCREMENT
        else:
            # Possibilities:
            # any
            return UpdateOperations.SET
