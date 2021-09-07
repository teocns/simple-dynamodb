import math
import re
allowed_limits_margin = 1.1


class ConditionExpression:
    Value: str

    # The reason we MUST pass here 

    ParsedAttributeNames = []
    ParsedAttributeValues = []
    

    __OriginalAttributeNames = []
    __OriginalAttributeValues = []
    

    def get_original_attribute_names(self) -> list:
        return self.__OriginalAttributeNames
    
    def get_original_attribute_values(self) -> list:
        return self.__OriginalAttributeValues
        

    def __init__(
        self,
        ExpressionString: str,
    ):
        self.Value = ExpressionString
        """
            Please pass expressionstring with by using < # > and < : > for attribute names and values to make sure the interpreter recognizes them
        """
        # Set attribute names to all words starting with #
        self.__OriginalAttributeNames = list(set(
            re.findall(r'#\w+', ExpressionString)
        ))
        
        self.__OriginalAttributeValues = list(set(
            re.findall(r':\w+', ExpressionString)
        ))

        self.ParsedAttributeNames = [word.replace("#",'') for word in self.__OriginalAttributeNames]

        # Serialize AttributeValues to interpret if it's a string or a number
        
        for i in range(len(self.__OriginalAttributeValues)):
            purified = self.__OriginalAttributeValues[i].replace(":", "")

            # Determine if it's a number or a string
            if purified.isdigit():
                # Determine if it's an integer or a float
                if "." in purified:
                    self.ParsedAttributeValues.append(float(purified))
                else:
                    self.ParsedAttributeValues.append(int(purified))
            else:
                self.ParsedAttributeValues.append(str(purified))


    def replace_with_expression_attribute_keys(self, ExpressionAttributeNameMappings: dict, ExpressionAttributeValueMappings: dict) -> str:
        """
            This method replaces the attribute names with the corresponding keys
            Returns the generated expression
        """
        
        for original_attribute_name in ExpressionAttributeNameMappings:
            generated_attribute_name_key = ExpressionAttributeNameMappings[original_attribute_name]
            self.Value = self.Value.replace("#"+original_attribute_name, generated_attribute_name_key)
    
        for original_attribute_value in ExpressionAttributeValueMappings:
            generated_attribute_value_key = ExpressionAttributeValueMappings[original_attribute_value]
            self.Value = self.Value.replace(':'+str(original_attribute_value), generated_attribute_value_key)


            