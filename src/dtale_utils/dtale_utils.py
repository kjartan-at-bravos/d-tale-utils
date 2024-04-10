import pandas as pd
import zipfile
import tempfile
import dtale
import os

def zipped_parquet_to_dtale(zip_file_path):
    """
    Extracts Parquet files from a ZIP archive and loads each into a D-Tale session.
    
    Each Parquet file within the ZIP archive is read into a Pandas DataFrame,
    and a D-Tale session is launched for it. The name of the session is set to
    the name of the Parquet file (without the file extension). The function
    prints the URLs for each D-Tale session.

    Parameters
    ----------
    zip_file_path : str
        The file path to the ZIP archive containing Parquet files.

    Returns
    -------
    None
        This function does not return a value. It prints the URL for each
        D-Tale session created for the Parquet files extracted from the ZIP archive.

    Examples
    --------
    >>> zipped_parquet_to_dtale('/tmp/dtale.zip')
    D-Tale session for dataset1 available at: http://localhost:40000
    D-Tale session for dataset2 available at: http://localhost:40001
    """
    # Using a temporary directory to extract the Parquet files
    with tempfile.TemporaryDirectory() as extract_dir:
        # Unzipping the Parquet files
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)

        # List all the Parquet files extracted
        parquet_files = [f for f in os.listdir(extract_dir) if f.endswith('.parquet')]

        # Loop through each Parquet file, read the data, and start a D-Tale session
        for file_name in parquet_files:
            pth_ = os.path.join(extract_dir, file_name)
            # Use the file name (without the extension) as the name of the D-Tale session
            session_name = os.path.splitext(file_name)[0]
            d = dtale.show_parquet(path=pth_, name=session_name)

        print(f"D-Tale available at: {d._main_url}")
