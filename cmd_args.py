import sys
import argparse

def cmdline_args():
    p = argparse.ArgumentParser(description='Specify the data file to process')
    p.add_argument(
        '-f', '--filename', required=True, type=str, help='Add a valid path'         
    )
    return(p.parse_args())
