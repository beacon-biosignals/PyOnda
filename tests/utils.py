import pandas as pd
import ast

def load_saved_processed_pandas(filepath):
    """We provide a utility function to load the output of
    arrow_to_processed_pandas if it was saved on disk

    We need to format some of the columns for the loaded table
    to match the output arrow_to_processed_pandas

    Parameters
    ----------
    filepath : str or Path
        path to the csv file
    """
    # If we load without dtype specification, most of the
    # column dtypes might be inferred as just object types
    df = pd.read_csv(filepath)

    # Typically if a column value contains a list, the value
    # will be read as a string by default
    def literal_eval(x):
        return ast.literal_eval(x) if not pd.isna(x) else x

    for col in df.columns:
        try:
            df[col] = df[col].map(literal_eval)
        except:
            continue

    return df
