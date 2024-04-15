import os
import signal
import zipfile
import tempfile
import time
import re
import socket
import click
import dtale
import dtale.global_state as global_state

global_state.set_chart_settings({'scatter_points': 150_000,})

DTYPE_SESSIONS = []

def signal_handler(signal, frame):
    print("\nShutting down d-tale")

    for d in DTYPE_SESSIONS:
        d.kill()

    exit(0)

signal.signal(signal.SIGINT, signal_handler)


def find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))  # Bind to a free port provided by the host.
        return s.getsockname()[1]  # Return the port number assigned.

def process_symbols(ctx, param, value):
    if value is None:
        return set()  # Return an empty set if no names are provided
    return set(s.strip() for s in value.split(','))


@click.command()
@click.argument("zip_path")
@click.option('--symbols', help='Symbols to load from the zip', type=str, required=False, callback=process_symbols)
def main(zip_path, symbols):
    """
    Simple CLI for loading zipped parquets into dtale
    """
    click.echo(f'Loading dataframes from {zip_path}')

    DTYPE_SESSIONS = zipped_parquet_to_dtale(zip_path, symbols_to_include=symbols)
    [print(d._main_url) for d in DTYPE_SESSIONS]
    DTYPE_SESSIONS[-1].open_browser()

    print("Running. Press Ctrl+C to exit.")
    while True:
        # The program does nothing here and waits to be killed
        time.sleep(1)



def zipped_parquet_to_dtale(zip_file_path, symbols_to_include: set[str]):
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
    >>> zipped_parquet_to_dtale('/tmp/dtale.zip', symbols_to_include={'I_py', 'P_py'})
    """
    # Using a temporary directory to extract the Parquet files

    with tempfile.TemporaryDirectory() as extract_dir:
        # Unzipping the Parquet files
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)

        # List all the Parquet files extracted
        parquet_files = [f for f in os.listdir(extract_dir) if f.endswith('.parquet')]

        ds = []
        # Loop through each Parquet file, read the data, and start a D-Tale session
        for i_, file_name in enumerate(parquet_files):
            pth_ = os.path.join(extract_dir, file_name)
            # Use the file name (without the extension) as the name of the D-Tale session
            symbol_name = os.path.splitext(file_name)[0]
            if (len(symbols_to_include) > 0) and (symbol_name not in symbols_to_include):
                continue

            name_ = replace_non_alphanumeric_with_space(symbol_name)
            print(f"Loaded {name_}")
            ds.append(dtale.show_parquet(path=pth_, name=name_, subprocess=True, port=find_free_port()))


    return ds



def replace_non_alphanumeric_with_space(s):
    """
    Replace all characters in the input string that are not alphanumeric (letters or numbers)
    with spaces.

    Parameters
    ----------
    s : str
        The input string to be processed.

    Returns
    -------
    str
        A new string with all non-alphanumeric characters replaced by spaces.

    Examples
    --------
    >>> text = "Hello, World! How are you today? @2024"
    >>> replace_non_alphanumeric_with_space(text)
    'Hello  World  How are you today   2024'

    Note
    ----
    Alphanumeric characters are defined as letters (both uppercase and lowercase) and digits.
    """
    pattern = r'[^a-zA-Z0-9]'
    return re.sub(pattern, ' ', s)



if __name__ == '__main__':
    main()

