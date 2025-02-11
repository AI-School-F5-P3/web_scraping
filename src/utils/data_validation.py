import os
import pandas as pd

def find_excel_file(filename):
    """
    Find Excel file in parent directories
    
    Args:
        filename (str): Name of Excel file
    
    Returns:
        str: Full path to the Excel file or None
    """
    current_dir = os.path.abspath(os.getcwd())
    
    possible_paths = [
        os.path.join(current_dir, filename),
        os.path.join(current_dir, 'data', filename),
        os.path.join(current_dir, '..', 'data', filename),
        os.path.join(current_dir, '..', '..', 'data', filename),
        os.path.join(current_dir, 'WEB_SCRAPING', 'data', filename)
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            print(f"File found at: {path}")
            return path
    
    return None

def validate_data(df):
    """
    Validate DataFrame before insertion
    
    Args:
        df (pandas.DataFrame): Input DataFrame
    
    Returns:
        pandas.DataFrame: Validated and normalized DataFrame
    """
    # Convert COD_INFOTEL to numeric
    df['COD_INFOTEL'] = pd.to_numeric(df['COD_INFOTEL'], errors='raise')
    
    # Check unique and non-null values in COD_INFOTEL
    if df['COD_INFOTEL'].isnull().any():
        raise ValueError("Null values found in COD_INFOTEL column")
    
    if df['COD_INFOTEL'].duplicated().any():
        raise ValueError("Duplicate values found in COD_INFOTEL column")
        
    print("COD_INFOTEL validation completed: no nulls and unique values")
    
    # Normalize COD_POSTAL
    df['COD_POSTAL'] = df['COD_POSTAL'].astype(str).str.zfill(5)
    print("COD_POSTAL normalization completed")
    
    # Convert empty strings to None
    string_columns = ['NIF', 'RAZON_SOCIAL', 'DOMICILIO', 'NOM_POBLACION', 'NOM_PROVINCIA', 'URL']
    for col in string_columns:
        df[col] = df[col].replace('', None)
        df[col] = df[col].where(pd.notnull(df[col]), None)
    
    return df

def prepare_dataframe(df):
    """
    Prepare DataFrame by adding new columns with default values
    
    Args:
        df (pandas.DataFrame): Input DataFrame
    
    Returns:
        pandas.DataFrame: Prepared DataFrame
    """
    # Add new columns with default values
    new_columns = {
        'URL_EXISTS': False,
        'URL_LIMPIA': None,
        'URL_STATUS': None,
        'URL_STATUS_MENSAJE': None,
        'TELEFONO_1': None,
        'TELEFONO_2': None,
        'TELEFONO_3': None,
        'FACEBOOK': None,
        'TWITTER': None,
        'LINKEDIN': None,
        'INSTAGRAM': None,
        'YOUTUBE': None,
        'E_COMMERCE': False
    }
    
    for col, default_value in new_columns.items():
        if col not in df.columns:
            df[col] = default_value

    # Validate URLs with blank spaces
    df['URL'] = df['URL'].apply(lambda x: '' if pd.isna(x) or (isinstance(x, str) and x.strip() == '') else x)
    
    # Set URL_EXISTS based on URL column
    df['URL_EXISTS'] = df['URL'].apply(lambda x: bool(x and x.strip()))

    # Ensure boolean columns are correct type
    df['URL_EXISTS'] = df['URL_EXISTS'].astype(bool)
    df['E_COMMERCE'] = df['E_COMMERCE'].astype(bool)
    
    # Convert empty strings to None in new columns
    string_columns = ['URL_LIMPIA', 'URL_STATUS_MENSAJE', 'TELEFONO_1', 'TELEFONO_2', 'TELEFONO_3', 
                    'FACEBOOK', 'TWITTER', 'LINKEDIN', 'INSTAGRAM', 'YOUTUBE']
    for col in string_columns:
        df[col] = df[col].replace('', None)
        df[col] = df[col].where(pd.notnull(df[col]), None)
            
    return df

def validate_and_prepare_dataframe(filename):
    """
    Comprehensive DataFrame validation and preparation
    
    Args:
        filename (str): Excel file name
    
    Returns:
        pandas.DataFrame: Validated and prepared DataFrame
    """
    # Find Excel file
    excel_path = find_excel_file(filename)

    if not excel_path:
        raise FileNotFoundError(f"Could not find file: {filename}")

    # Read Excel file
    print("Reading Excel file...")
    df = pd.read_excel(excel_path)
    print(f"Read {len(df)} records from Excel")

    # Validate and normalize data
    df = validate_data(df)
    
    # Prepare DataFrame with new columns
    df = prepare_dataframe(df)

    return df