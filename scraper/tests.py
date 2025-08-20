

def validate_data(data_list, schema):
    """
    Validates a dictionary against a schema of expected types.
    Missing data (None) is allowed.
    
    Args:
        data_dict (dict): The scraped data to check.
        schema (dict): Expected types, e.g., {'gameDuration': int, 'kills': int}

    Returns:
        (bool, list): (is_valid, errors)
    """
    invalid_entries = []

    for i, data_dict in enumerate(data_list):
        errors = []

        for key, expected_type in schema.items():
            value = data_dict.get(key)

            if value is None:  
                continue  # Missing data is fine

            if not isinstance(value, expected_type):
                errors.append(f"{key} should be {expected_type.__name__}, got {type(value).__name__}")

        if errors:
            invalid_entries.append((errors))

            return invalid_entries

    return None

    