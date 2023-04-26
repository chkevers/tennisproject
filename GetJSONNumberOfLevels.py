import json

def count_levels(json_data):
    # if the JSON data is a primitive data type, return 0
    if not isinstance(json_data, (dict, list)):
        return 0

    # if the JSON data is a list, find the maximum number of levels in its elements
    if isinstance(json_data, list):
        return max(count_levels(element) for element in json_data) + 1

    # if the JSON data is a dictionary, find the maximum number of levels in its values
    if isinstance(json_data, dict):
        return max(count_levels(value) for value in json_data.values()) + 1

# example JSON data


# parse the JSON data
# parsed_data = json.loads(json.dumps(json_data))

# # count the number of levels in the JSON data
# num_levels = count_levels(parsed_data)

# print(num_levels) # prints 2
